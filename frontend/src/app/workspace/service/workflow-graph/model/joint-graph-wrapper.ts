/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { fromEvent, Observable, ReplaySubject, Subject } from "rxjs";
import { filter, map } from "rxjs/operators";
import {
  LogicalPort,
  OperatorLink,
  OperatorPredicate,
  Point,
  WorkflowMacro,
} from "../../../types/workflow-common.interface";
import * as joint from "jointjs";
import * as dagre from "dagre";
import * as graphlib from "graphlib";
import { ObservableContextManager } from "src/app/common/util/context";
import { Coeditor, User } from "../../../../common/type/user";
import {
  JointUIService,
  operatorCoeditorChangedPropertyClass,
  operatorCoeditorEditingClass,
} from "../../joint-ui/joint-ui.service";
import { dia } from "jointjs/types/joint";
import * as _ from "lodash";
import Selectors = dia.Cell.Selectors;

type linkIDType = { linkID: string };

type JointModelEventInfo = {
  add: boolean;
  merge: boolean;
  remove: boolean;
  changes: {
    added: joint.dia.Cell[];
    merged: joint.dia.Cell[];
    removed: joint.dia.Cell[];
  };
};

// argument type of callback event on a JointJS Model,
// which is a 3-element tuple:
// 1. the JointJS model (Cell) of the event
// 2 and 3. additional information of the event
type JointModelEvent = [joint.dia.Cell, { graph: joint.dia.Graph; models: joint.dia.Cell[] }, JointModelEventInfo];

type JointLinkChangeEvent = [joint.dia.Link, { x: number; y: number }, { ui: boolean; updateConnectionOnly: boolean }];

type JointPositionChangeEvent = [joint.dia.Element, { x: number; y: number }, { macroVisualSync?: boolean }?];

type PositionInfo = {
  currPos: Point;
  lastPos: Point | undefined;
};

export type JointHighlights = Readonly<{
  operators: readonly string[];
  links: readonly string[];
  commentBoxes: readonly string[];
  ports: readonly LogicalPort[];
}>;

export type JointGraphContextType = Readonly<{
  async: boolean;
}>;
const DefaultContext: JointGraphContextType = {
  async: false,
};

/**
 * JointGraphWrapper wraps jointGraph to provide:
 *  - getters of the properties (to hide the methods that could alther the jointGraph directly)
 *  - event streams of JointGraph in RxJS Observables (instead of the callback functions to fit our use of RxJS)
 *
 * JointJS Graph only contains information related the UI, such as:
 *  - position of operator elements
 *  - events of a cell (operator or link) being dragging around
 *  - events of adding/deleting a link on the UI,
 *      this doesn't necessarily corresponds to adding/deleting a link logically on the graph
 *      because the link might not connect to a target operator while user is dragging the link
 *
 * If an external module needs to access more properties of JointJS graph,
 *  or to make changes **irrelevant** to the graph data structure, but related direcly to the UI,
 *  (such as changing the color of an operator), more methods can be added in this class.
 *
 * For an overview of the services in WorkflowGraphModule, see workflow-graph-design.md
 */
export class JointGraphWrapper {
  private static readonly MACRO_FRAME_PREFIX = "texera-macro-frame-";
  private static readonly MACRO_NODE_PREFIX = "texera-macro-node-";
  private static readonly MACRO_PROXY_LINK_PREFIX = "texera-macro-proxy-link-";
  private static readonly MACRO_FRAME_PADDING_X = 43;
  private static readonly MACRO_NODE_WIDTH = 150;
  private static readonly MACRO_NODE_HEIGHT = 48;
  private static readonly MACRO_INTERNAL_TOP_GAP = 37;
  private static readonly MACRO_FRAME_PADDING_TOP =
    JointGraphWrapper.MACRO_NODE_HEIGHT + JointGraphWrapper.MACRO_INTERNAL_TOP_GAP;
  private static readonly MACRO_FRAME_PADDING_BOTTOM = 70;
  private static readonly MACRO_NODE_INSET = 0;
  private static readonly MACRO_NODE_VIEW_KEY = "texeraMacroNodeView";

  // zoom diff represents the ratio that is zoom in/out everytime, for clicking +/- buttons or using mousewheel
  public static readonly ZOOM_CLICK_DIFF: number = 0.05;
  public static readonly INIT_ZOOM_VALUE: number = 1;

  public static readonly ZOOM_MINIMUM: number = 0.7;
  public static readonly ZOOM_MAXIMUM: number = 1.3;

  public jointGraphContext = JointGraphWrapper.jointGraphContextFactory();
  public mainPaper!: joint.dia.Paper;

  private mainJointPaperAttachedStream: Subject<joint.dia.Paper> = new ReplaySubject(1);

  private elementPositions: Map<string, PositionInfo> = new Map<string, PositionInfo>();
  private listenPositionChange: boolean = true;

  // flag that indicates whether multiselect mode is on
  private multiSelect: boolean = false;

  private reloadingWorkflow: boolean = false;
  private collapsedMacroIDs = new Set<string>();
  private workflowMacros: readonly WorkflowMacro[] = [];
  private workflowLinks: readonly OperatorLink[] = [];

  // the currently highlighted operators' IDs
  private currentHighlightedOperators: string[] = [];
  // event stream of highlighting an operator
  private jointOperatorHighlightStream = new Subject<readonly string[]>();
  // event stream of un-highlighting an operator
  private jointOperatorUnhighlightStream = new Subject<readonly string[]>();
  // event stream of highlighting a group
  private jointGroupHighlightStream = new Subject<readonly string[]>();
  // event stream of un-highlighting a group
  private jointGroupUnhighlightStream = new Subject<readonly string[]>();
  // event stream of highlighing a link
  private jointLinkHighlightStream = new Subject<readonly string[]>();
  // event stream of unhighlighing a link
  private jointLinkUnhighlightStream = new Subject<readonly string[]>();

  private jointCommentBoxHighlightStream = new Subject<readonly string[]>();

  private jointCommentBoxUnhighlightStream = new Subject<readonly string[]>();

  private jointPortHighlightStream = new Subject<readonly LogicalPort[]>();

  private jointPortUnhighlightStream = new Subject<readonly LogicalPort[]>();

  private currentHighlightedCommentBoxes: string[] = [];

  // event stream of zooming the jointJS paper
  private workflowEditorZoomSubject: Subject<number> = new Subject<number>();
  // event stream of restoring zoom / offset default of the jointJS paper
  private restorePaperOffsetSubject: Subject<void> = new Subject<void>();

  // event stream of showing the breakpoint button of a link
  private jointLinkBreakpointShowStream = new Subject<linkIDType>();
  // event stream of hiding the breakpoint button of a link
  private jointLinkBreakpointHideStream = new Subject<linkIDType>();
  // the currently highlighted links' ids
  private currentHighlightedLinks: string[] = [];
  // the linkIDs of those links with a breakpoint

  private currentHighlightedPorts: LogicalPort[] = [];
  // the IDs of ports currently being edited
  private linksWithBreakpoints: string[] = [];

  // current zoom ratio
  private zoomRatio: number = JointGraphWrapper.INIT_ZOOM_VALUE;

  /**
   * This will capture all events in JointJS
   *  involving the 'add' operation
   */
  private jointCellAddStream = fromEvent<JointModelEvent>(this.jointGraph, "add").pipe(map(value => value[0]));
  /**
   * This will capture all events in JointJS
   *  involving the 'remove' operation
   */
  private jointCellDeleteStream = fromEvent<JointModelEvent>(this.jointGraph, "remove").pipe(map(value => value[0]));

  constructor(public jointGraph: joint.dia.Graph) {
    // handle if the currently highlighted operator/group/link is deleted, it should be unhighlighted
    this.handleElementDeleteUnhighlight();

    this.jointCellAddStream.pipe(filter(cell => cell.isElement())).subscribe(element => {
      const initPosition = {
        currPos: (element as joint.dia.Element).position(),
        lastPos: undefined,
      };
      this.elementPositions.set(element.id.toString(), initPosition);
    });

    this.jointCellDeleteStream
      .pipe(filter(cell => cell.isElement()))
      .subscribe(element => this.elementPositions.delete(element.id.toString()));
  }

  /**
   * Let the JointGraph model be attached to the joint paper (paperOptions will be passed to Joint Paper constructor).
   *
   * We don't want to expose JointModel as a public variable, so instead we let JointPaper to pass the constructor options,
   *  and JointModel can be still attached to it without being publicly accessible by other modules.
   *
   * @param paperOptions JointJS paper options
   */
  public attachMainJointPaper(paperOptions: joint.dia.Paper.Options): joint.dia.Paper {
    paperOptions.model = this.jointGraph;
    const paper = new joint.dia.Paper(paperOptions);
    this.mainPaper = paper;
    this.mainJointPaperAttachedStream.next(this.mainPaper);
    this.jointGraphContext.attachPaper(paper);
    return paper;
  }

  public getMainJointPaper(): joint.dia.Paper {
    return this.mainPaper;
  }

  public getMainJointPaperAttachedStream(): Observable<joint.dia.Paper> {
    return this.mainJointPaperAttachedStream;
  }

  /**
   * This method is used to toggle the multiselect mode.
   * @param multiSelect
   */
  public setMultiSelectMode(multiSelect: boolean): void {
    this.multiSelect = multiSelect;
  }

  public setReloadingWorkflow(reloadingWorkflow: boolean): void {
    this.reloadingWorkflow = reloadingWorkflow;
  }

  public getReloadingWorkflow(): boolean {
    return this.reloadingWorkflow;
  }

  /**
   * Gets the operator ID of the current highlighted operators.
   * Returns an empty list if there is no highlighted operator.
   *
   * The returned array is not the original one so that other
   * services/components can't modify it directly.
   */
  public getCurrentHighlightedOperatorIDs(): readonly string[] {
    return this.currentHighlightedOperators;
  }

  /**
   * get the ids of all the links that are currently highlighted
   */
  public getCurrentHighlightedLinkIDs(): readonly string[] {
    return this.currentHighlightedLinks;
  }

  public getCurrentHighlightedPortIDs(): readonly LogicalPort[] {
    return this.currentHighlightedPorts;
  }

  public getCurrentHighlightedCommentBoxIDs(): readonly string[] {
    return this.currentHighlightedCommentBoxes;
  }

  public getCurrentHighlights(): JointHighlights {
    return {
      operators: this.currentHighlightedOperators,
      links: this.currentHighlightedLinks,
      commentBoxes: this.currentHighlightedCommentBoxes,
      ports: this.currentHighlightedPorts,
    };
  }

  public static getMacroFrameID(macroId: string): string {
    return `${JointGraphWrapper.MACRO_FRAME_PREFIX}${macroId}`;
  }

  public static getMacroNodeID(macroId: string): string {
    return `${JointGraphWrapper.MACRO_NODE_PREFIX}${macroId}`;
  }

  public static getMacroIDFromNodeID(elementID: string): string {
    return elementID.substring(JointGraphWrapper.MACRO_NODE_PREFIX.length);
  }

  public static isMacroFrameID(elementID: string): boolean {
    return elementID.startsWith(JointGraphWrapper.MACRO_FRAME_PREFIX);
  }

  public static isMacroNodeID(elementID: string): boolean {
    return elementID.startsWith(JointGraphWrapper.MACRO_NODE_PREFIX);
  }

  public static isMacroElementID(elementID: string): boolean {
    return JointGraphWrapper.isMacroFrameID(elementID) || JointGraphWrapper.isMacroNodeID(elementID);
  }

  public static isMacroProxyLinkID(linkID: string): boolean {
    return linkID.startsWith(JointGraphWrapper.MACRO_PROXY_LINK_PREFIX);
  }

  public getCurrentHighlightedIDs(): readonly string[] {
    return [
      ...this.currentHighlightedOperators,
      ...this.currentHighlightedLinks,
      ...this.currentHighlightedCommentBoxes,
    ];
  }

  /**
   * Returns an Observable stream capturing the element position change event in JointJS graph.
   * An element can be an operator or a group.
   *
   * - elementID: the moved element's ID
   * - oldPosition: the element's position before moving
   * - newPosition: where the element is moved to
   */
  public getElementPositionChangeEvent(): Observable<{
    elementID: string;
    oldPosition: Point;
    newPosition: Point;
  }> {
    return fromEvent<JointPositionChangeEvent>(this.jointGraph, "change:position").pipe(
      filter(e => !JointGraphWrapper.isMacroElementID(e[0].id.toString())),
      map(e => {
        const elementID = e[0].id.toString();
        const oldPosition = this.elementPositions.get(elementID);
        const newPosition = { x: e[1].x, y: e[1].y };
        if (!oldPosition) {
          throw new Error(`internal error: cannot find element position for ${elementID}`);
        }
        if (
          !oldPosition.lastPos ||
          oldPosition.currPos.x !== newPosition.x ||
          oldPosition.currPos.y !== newPosition.y
        ) {
          oldPosition.lastPos = oldPosition.currPos;
        }
        this.elementPositions.set(elementID, {
          currPos: newPosition,
          lastPos: oldPosition.lastPos,
        });
        return {
          elementID: elementID,
          oldPosition: oldPosition.lastPos,
          newPosition: newPosition,
        };
      })
    );
  }

  public getMacroNodePositionChangeEvent(): Observable<{ macroID: string; oldPosition: Point; newPosition: Point }> {
    return fromEvent<JointPositionChangeEvent>(this.jointGraph, "change:position").pipe(
      filter(e => JointGraphWrapper.isMacroNodeID(e[0].id.toString())),
      filter(e => !(e[2]?.macroVisualSync ?? false)),
      map(e => {
        const elementID = e[0].id.toString();
        const newPosition = { x: e[1].x, y: e[1].y };
        const oldPosition = this.elementPositions.get(elementID);
        const previousPosition = oldPosition?.currPos ?? newPosition;
        this.elementPositions.set(elementID, {
          currPos: newPosition,
          lastPos: previousPosition,
        });
        return {
          macroID: JointGraphWrapper.getMacroIDFromNodeID(elementID),
          oldPosition: previousPosition,
          newPosition,
        };
      })
    );
  }

  public unhighlightElements(elements: JointHighlights): void {
    this.unhighlightOperators(...elements.operators);
    this.unhighlightLinks(...elements.links);
    this.unhighlightCommentBoxes(...elements.commentBoxes);
    this.unhighlightPorts(...elements.ports);
  }

  /**
   * Highlights operators in the given list.
   *
   * Emits an event to the operator highlight stream with a list of operatorIDs
   * that are highlighted.
   *
   * @param operatorIDs
   */
  public highlightOperators(...operatorIDs: string[]): void {
    const highlightedOperatorIDs: string[] = [];
    operatorIDs.forEach(operatorID => {
      this.highlightElement(operatorID, this.currentHighlightedOperators, highlightedOperatorIDs);
    });

    if (highlightedOperatorIDs.length > 0) {
      this.jointOperatorHighlightStream.next(highlightedOperatorIDs);
    }
  }

  /**
   * Unhighlights operators in the given list.
   *
   * Emits an event to the operator unhighlight stream with a list of operatorIDs
   * that are unhighlighted.
   *
   * @param operatorIDs
   */
  public unhighlightOperators(...operatorIDs: string[]): void {
    const unhighlightedOperatorIDs: string[] = [];
    operatorIDs.forEach(operatorID =>
      this.unhighlightElement(operatorID, this.currentHighlightedOperators, unhighlightedOperatorIDs)
    );

    if (unhighlightedOperatorIDs.length > 0) {
      this.jointOperatorUnhighlightStream.next(unhighlightedOperatorIDs);
    }
  }

  /**
   * Highlights the link with given linkID.
   * Emits an event to the link highlight stream.
   * @param linkIDs
   */
  public highlightLinks(...linkIDs: string[]): void {
    const highlightedLinkIDs: string[] = [];
    linkIDs.forEach(linkID => this.highlightElement(linkID, this.currentHighlightedLinks, highlightedLinkIDs));
    if (highlightedLinkIDs.length > 0) {
      this.jointLinkHighlightStream.next(highlightedLinkIDs);
    }
  }

  /**
   * Unhighlights the given highlighted link.
   * Emits an event to the link unhighlight stream.
   * @param linkIDs
   */
  public unhighlightLinks(...linkIDs: string[]): void {
    const unhighlightedLinkIDs: string[] = [];
    linkIDs.forEach(linkID => this.unhighlightElement(linkID, this.currentHighlightedLinks, unhighlightedLinkIDs));
    if (unhighlightedLinkIDs.length > 0) {
      this.jointLinkUnhighlightStream.next(unhighlightedLinkIDs);
    }
  }

  public highlightCommentBoxes(...commentBoxIDs: string[]): void {
    const highlightedCommentBoxesIDs: string[] = [];
    commentBoxIDs.forEach(commentBoxID =>
      this.highlightElement(commentBoxID, this.currentHighlightedCommentBoxes, highlightedCommentBoxesIDs)
    );
    if (highlightedCommentBoxesIDs.length > 0) {
      this.jointCommentBoxHighlightStream.next(highlightedCommentBoxesIDs);
    }
  }

  public unhighlightCommentBoxes(...commentBoxIDs: string[]): void {
    const unhighlightedCommentBoxesIDs: string[] = [];
    commentBoxIDs.forEach(commentBoxID =>
      this.unhighlightElement(commentBoxID, this.currentHighlightedCommentBoxes, unhighlightedCommentBoxesIDs)
    );
    if (unhighlightedCommentBoxesIDs.length > 0) {
      this.jointCommentBoxUnhighlightStream.next(unhighlightedCommentBoxesIDs);
    }
  }

  public highlightPorts(...operatorPortIDs: LogicalPort[]): void {
    const highlightedLogicalPortIDs: LogicalPort[] = [];
    operatorPortIDs
      .filter(operatorPortID => _.find(this.currentHighlightedPorts, operatorPortID) === undefined)
      .forEach(operatorPortID => {
        if (!this.multiSelect) this.unhighlightPorts(...this.currentHighlightedPorts);
        this.currentHighlightedPorts.push(operatorPortID);
        highlightedLogicalPortIDs.push(operatorPortID);
      });
    this.jointPortHighlightStream.next(highlightedLogicalPortIDs);
  }

  public unhighlightPorts(...operatorPortIDs: LogicalPort[]): void {
    const unhighlightedLogicalPortIDs: LogicalPort[] = [];
    operatorPortIDs
      .filter(operatorPortID => _.find(this.currentHighlightedPorts, operatorPortID) !== undefined)
      .forEach(operatorPortID => {
        this.currentHighlightedPorts.splice(_.indexOf(this.currentHighlightedPorts, operatorPortID), 1);
        unhighlightedLogicalPortIDs.push(operatorPortID);
      });
    this.jointPortUnhighlightStream.next(unhighlightedLogicalPortIDs);
  }

  /**
   * Gets the event stream of an operator being highlighted.
   */
  public getJointOperatorHighlightStream(): Observable<readonly string[]> {
    return this.jointOperatorHighlightStream.pipe(this.jointGraphContext.bufferWhileAsync);
  }

  /**
   * Gets the event stream of an operator being unhighlighted.
   * The operator could be unhighlighted because it's deleted.
   */
  public getJointOperatorUnhighlightStream(): Observable<readonly string[]> {
    return this.jointOperatorUnhighlightStream.pipe(this.jointGraphContext.bufferWhileAsync);
  }

  /**
   * get the ids of all the links that have a breakpoint
   */
  public getLinkIDsWithBreakpoint(): readonly string[] {
    return this.linksWithBreakpoints;
  }

  /**
   * get the event stream of a link being highlighted.
   */
  public getLinkHighlightStream(): Observable<readonly string[]> {
    return this.jointLinkHighlightStream.pipe(this.jointGraphContext.bufferWhileAsync);
  }

  /**
   * get the event stream of a link being unhighlighted.
   */
  public getLinkUnhighlightStream(): Observable<readonly string[]> {
    return this.jointLinkUnhighlightStream.pipe(this.jointGraphContext.bufferWhileAsync);
  }

  /**
   * get the event stream of showing the breakpoint button of a link
   */
  public getLinkBreakpointShowStream(): Observable<linkIDType> {
    return this.jointLinkBreakpointShowStream.asObservable();
  }

  /**
   * get the event stream of hiding the breakpoint button of a link
   */
  public getLinkBreakpointHideStream(): Observable<linkIDType> {
    return this.jointLinkBreakpointHideStream.asObservable();
  }

  /**
   * Gets the event stream of an operator being dragged.
   */
  public getJointGroupHighlightStream(): Observable<readonly string[]> {
    return this.jointGroupHighlightStream.pipe(this.jointGraphContext.bufferWhileAsync);
  }

  /**
   * Gets the event stream of a group being unhighlighted.
   * The group could be unhighlighted because it's deleted.
   */
  public getJointGroupUnhighlightStream(): Observable<readonly string[]> {
    return this.jointGroupUnhighlightStream.asObservable().pipe(this.jointGraphContext.bufferWhileAsync);
  }

  public getJointCommentBoxHighlightStream(): Observable<readonly string[]> {
    return this.jointCommentBoxHighlightStream.asObservable();
  }

  public getJointCommentBoxUnhighlightStream(): Observable<readonly string[]> {
    return this.jointCommentBoxUnhighlightStream.asObservable();
  }

  public getJointPortHighlightStream(): Observable<readonly LogicalPort[]> {
    return this.jointPortHighlightStream.asObservable();
  }

  public getJointPortUnhighlightStream(): Observable<readonly LogicalPort[]> {
    return this.jointPortUnhighlightStream.asObservable();
  }
  /**
   * Returns an Observable stream capturing the element cell delete event in JointJS graph.
   * An element cell can be an operator or an group.
   */
  public getJointElementCellDeleteStream(): Observable<joint.dia.Element> {
    return this.jointCellDeleteStream.pipe(
      filter(cell => cell.isElement()),
      map(cell => <joint.dia.Element>cell)
    );
  }

  /**
   * Returns an Observable stream capturing the link cell add event in JointJS graph.
   *
   * Notice that a link added to JointJS graph doesn't mean it will be added to Texera Workflow Graph as well
   *  because the link might not be valid (not connected to a target operator and port yet).
   * This event only represents that a link cell is visually added to the UI.
   *
   */
  public getJointLinkCellAddStream(): Observable<joint.dia.Link> {
    return this.jointCellAddStream.pipe(
      filter(cell => cell.isLink()),
      map(cell => <joint.dia.Link>cell)
    );
  }

  /**
   * Returns an Observable stream capturing the link cell delete event in JointJS graph.
   *
   * Notice that a link deleted from JointJS graph doesn't mean the same event happens for Texera Workflow Graph
   *  because the link might not be valid and doesn't exist logically in the Workflow Graph.
   * This event only represents that a link cell visually disappears from the UI.
   *
   */
  public getJointLinkCellDeleteStream(): Observable<joint.dia.Link> {
    return this.jointCellDeleteStream.pipe(
      filter(cell => cell.isLink()),
      map(cell => <joint.dia.Link>cell)
    );
  }

  /**
   * This method will update the zoom ratio, which will be used
   *  in calculating the position of the operator dropped on the UI.
   *
   * @param ratio new ratio from zooming
   */
  public setZoomProperty(ratio: number): void {
    this.zoomRatio = ratio;
    this.workflowEditorZoomSubject.next(this.zoomRatio);
  }

  /**
   * Check if the zoom ratio reaches the minimum.
   */
  public isZoomRatioMin(): boolean {
    return this.zoomRatio <= JointGraphWrapper.ZOOM_MINIMUM;
  }

  /**
   * Check if the zoom ratio reaches the maximum.
   */
  public isZoomRatioMax(): boolean {
    return this.zoomRatio >= JointGraphWrapper.ZOOM_MAXIMUM;
  }

  /**
   * Returns an observable stream containing the new zoom ratio
   *  for the jointJS paper.
   */
  public getWorkflowEditorZoomStream(): Observable<number> {
    return this.workflowEditorZoomSubject.asObservable();
  }

  /**
   * This method will fetch current zoom ratio of the paper.
   */
  public getZoomRatio(): number {
    return this.zoomRatio;
  }

  public autoLayoutJoint(): void {
    joint.layout.DirectedGraph.layout(
      [
        ...this.jointGraph
          .getElements()
          .filter(el => el.attributes.type !== "region" && !JointGraphWrapper.isMacroElementID(el.id.toString())),
        ...this.jointGraph.getLinks().filter(link => !JointGraphWrapper.isMacroProxyLinkID(link.id.toString())),
      ],
      {
        dagre: dagre,
        graphlib: graphlib,
        nodeSep: 100,
        edgeSep: 150,
        rankSep: 80,
        ranker: "tight-tree",
        rankDir: "LR",
        resizeClusters: true,
      }
    );
    this.refreshPaperViews();
  }

  public autoLayoutMacroInternals(
    macroID: string,
    operators: readonly OperatorPredicate[],
    links: readonly OperatorLink[],
    origin: Point
  ): string[] {
    const internalOperatorIDs = new Set(
      operators.filter(operator => operator.macroIdParent === macroID).map(operator => operator.operatorID)
    );
    const internalElements = Array.from(internalOperatorIDs)
      .map(operatorID => this.jointGraph.getCell(operatorID))
      .filter((cell): cell is joint.dia.Element => Boolean(cell?.isElement()));
    if (!internalElements.length) return [];

    const internalLinks = links
      .filter(
        link => internalOperatorIDs.has(link.source.operatorID) && internalOperatorIDs.has(link.target.operatorID)
      )
      .map(link => this.jointGraph.getCell(link.linkID))
      .filter((cell): cell is joint.dia.Link => Boolean(cell?.isLink()));

    joint.layout.DirectedGraph.layout([...internalElements, ...internalLinks], {
      dagre: dagre,
      graphlib: graphlib,
      nodeSep: 90,
      edgeSep: 100,
      rankSep: 80,
      ranker: "tight-tree",
      rankDir: "LR",
    });

    const minX = Math.min(...internalElements.map(element => element.position().x));
    const minY = Math.min(...internalElements.map(element => element.position().y));
    const offsetX = origin.x + JointGraphWrapper.MACRO_NODE_WIDTH + 70 - minX;
    const offsetY = origin.y + JointGraphWrapper.MACRO_NODE_HEIGHT + JointGraphWrapper.MACRO_INTERNAL_TOP_GAP - minY;
    internalElements.forEach(element => element.translate(offsetX, offsetY));
    this.refreshPaperViews();
    return internalElements.map(element => element.id.toString());
  }

  /**
   * This method will restore the default zoom ratio and offset for
   *  the jointjs paper by sending an event to restorePaperSubject.
   */
  public restoreDefaultZoomAndOffset(): void {
    this.setZoomProperty(JointGraphWrapper.INIT_ZOOM_VALUE);
    this.restorePaperOffsetSubject.next();
  }

  /**
   * Returns an Observable stream capturing the event of restoring
   *  default offset
   */
  public getRestorePaperOffsetStream(): Observable<void> {
    return this.restorePaperOffsetSubject.asObservable();
  }

  /**
   * Returns an Observable stream capturing the link cell delete event in JointJS graph.
   *
   * Notice that the link change event will be triggered whenever the link's source or target is changed:
   *  - one end of the link is attached to a port
   *  - one end of the link is detached to a port and become a point (coordinate) in the paper
   *  - one end of the link is moved from one point to another point in the paper
   */
  public getJointLinkCellChangeStream(): Observable<joint.dia.Link> {
    return fromEvent<JointLinkChangeEvent>(this.jointGraph, "change:source change:target").pipe(map(value => value[0]));
  }

  /**
   * This method will get the element position on the JointJS paper.
   * An element can be an operator or a group.
   */
  public getElementPosition(elementID: string): Point {
    const cell: joint.dia.Cell | undefined = this.jointGraph.getCell(elementID);
    if (!cell) {
      throw new Error(`element with ID ${elementID} doesn't exist`);
    }
    if (!cell.isElement()) {
      throw new Error(`${elementID} is not an element`);
    }
    const element = <joint.dia.Element>cell;
    const position = element.position();
    return { x: position.x, y: position.y };
  }

  public getMacroFramePosition(macroID: string): Point | undefined {
    const cell = this.jointGraph.getCell(JointGraphWrapper.getMacroFrameID(macroID));
    if (!cell?.isElement()) {
      return undefined;
    }
    const position = (cell as joint.dia.Element).position();
    return { x: position.x, y: position.y };
  }

  /**
   * This method repositions the element according to given offsets.
   * An element can be an operator or a group.
   */
  public setElementPosition(elementID: string, offsetX: number, offsetY: number): void {
    const cell: joint.dia.Cell | undefined = this.jointGraph.getCell(elementID);
    if (!cell) {
      throw new Error(`element with ID ${elementID} doesn't exist`);
    }
    if (!cell.isElement()) {
      throw new Error(`${elementID} is not an element`);
    }
    const element = <joint.dia.Element>cell;
    element.translate(offsetX, offsetY);
  }

  /**
   * This method repositions the element according to given absolute positions.
   * An element can be an operator or a group.
   */
  public setAbsolutePosition(elementID: string, posX: number, poY: number): void {
    const cell: joint.dia.Cell | undefined = this.jointGraph.getCell(elementID);
    if (!cell) {
      throw new Error(`element with ID ${elementID} doesn't exist`);
    }
    if (!cell.isElement()) {
      throw new Error(`${elementID} is not an element`);
    }
    const element = <joint.dia.Element>cell;
    element.position(posX, poY);
  }

  public refreshMacroFrames(
    operators: readonly OperatorPredicate[],
    macros?: readonly WorkflowMacro[],
    links?: readonly OperatorLink[]
  ): void {
    if (macros) this.workflowMacros = macros;
    if (links) this.workflowLinks = links;
    this.collapsedMacroIDs = new Set(this.workflowMacros.filter(macro => macro.collapsed).map(macro => macro.macroID));

    const operatorGroups = new Map<string, Point[]>();
    const expandedMacroBounds = new Map<string, { x: number; y: number; width: number; height: number }>();
    operators.forEach(operator => {
      const macroId = operator.macroIdParent?.trim();
      const cell = this.jointGraph.getCell(operator.operatorID);
      if (!macroId || this.collapsedMacroIDs.has(macroId) || !cell?.isElement()) return;
      const position = (cell as joint.dia.Element).position();
      operatorGroups.set(macroId, [...(operatorGroups.get(macroId) ?? []), { x: position.x, y: position.y }]);
    });

    const activeFrameIDs = new Set<string>();
    const activeNodeIDs = new Set<string>();
    operatorGroups.forEach((positions, macroId) => {
      const frameID = JointGraphWrapper.getMacroFrameID(macroId);
      activeFrameIDs.add(frameID);
      const minX = Math.min(...positions.map(pos => pos.x));
      const minY = Math.min(...positions.map(pos => pos.y));
      const maxX = Math.max(...positions.map(pos => pos.x + JointUIService.DEFAULT_OPERATOR_WIDTH));
      const maxY = Math.max(...positions.map(pos => pos.y + JointUIService.DEFAULT_OPERATOR_HEIGHT));
      const bounds = {
        x: minX - JointGraphWrapper.MACRO_FRAME_PADDING_X,
        y: minY - JointGraphWrapper.MACRO_FRAME_PADDING_TOP,
        width: maxX - minX + JointGraphWrapper.MACRO_FRAME_PADDING_X * 2,
        height: maxY - minY + JointGraphWrapper.MACRO_FRAME_PADDING_TOP + JointGraphWrapper.MACRO_FRAME_PADDING_BOTTOM,
      };
      expandedMacroBounds.set(macroId, bounds);
      const existingFrame = this.jointGraph.getCell(frameID);
      const frame = existingFrame?.isElement()
        ? (existingFrame as joint.dia.Element)
        : new joint.shapes.standard.Rectangle();

      frame.set("id", frameID);
      frame.position(bounds.x, bounds.y, { macroVisualSync: true } as any);
      frame.resize(bounds.width, bounds.height);
      JointGraphWrapper.styleMacroFrame(frame);
      if (!existingFrame) {
        this.jointGraph.addCell(frame);
      }
      frame.toBack();
    });

    if (this.workflowMacros) {
      this.workflowMacros.forEach(macro => {
        const nodeID = JointGraphWrapper.getMacroNodeID(macro.macroID);
        activeNodeIDs.add(nodeID);
        const collapsed = macro.collapsed ?? false;
        const viewMode = collapsed ? "collapsed" : "expanded";
        const existingNode = this.jointGraph.getCell(nodeID);
        let node =
          existingNode?.isElement() && existingNode.get(JointGraphWrapper.MACRO_NODE_VIEW_KEY) === viewMode
            ? (existingNode as joint.dia.Element)
            : undefined;
        if (!node) {
          existingNode?.remove();
          node = JointGraphWrapper.createMacroNode(collapsed);
          node.set("id", nodeID);
          node.set(JointGraphWrapper.MACRO_NODE_VIEW_KEY, viewMode);
          this.jointGraph.addCell(node);
        }
        const nodePosition =
          collapsed || !expandedMacroBounds.has(macro.macroID)
            ? macro.position
            : JointGraphWrapper.getExpandedMacroTabPosition(
                expandedMacroBounds.get(macro.macroID) as { x: number; y: number; width: number; height: number }
              );

        node.set("id", nodeID);
        node.position(nodePosition.x, nodePosition.y, { macroVisualSync: true } as any);
        node.resize(
          collapsed ? JointUIService.DEFAULT_OPERATOR_WIDTH : JointGraphWrapper.MACRO_NODE_WIDTH,
          collapsed ? JointUIService.DEFAULT_OPERATOR_HEIGHT : JointGraphWrapper.MACRO_NODE_HEIGHT
        );
        JointGraphWrapper.styleMacroNode(node, macro.name, collapsed);
        this.elementPositions.set(nodeID, { currPos: nodePosition, lastPos: undefined });
      });
    }

    this.jointGraph
      .getCells()
      .filter(cell => JointGraphWrapper.isMacroFrameID(cell.id.toString()) && !activeFrameIDs.has(cell.id.toString()))
      .forEach(cell => cell.remove());
    if (this.workflowMacros) {
      this.jointGraph
        .getCells()
        .filter(cell => JointGraphWrapper.isMacroNodeID(cell.id.toString()) && !activeNodeIDs.has(cell.id.toString()))
        .forEach(cell => cell.remove());
    }
    this.applyMacroCollapseState(operators);
    this.refreshMacroBoundaryConnections(operators, this.workflowLinks, this.workflowMacros);
    this.clearMacroSelectionChrome();
    this.refreshPaperViews();
  }

  private refreshMacroBoundaryConnections(
    operators: readonly OperatorPredicate[],
    links: readonly OperatorLink[],
    macros: readonly WorkflowMacro[]
  ): void {
    this.jointGraph
      .getLinks()
      .filter(link => JointGraphWrapper.isMacroProxyLinkID(link.id.toString()))
      .forEach(link => link.remove());

    const macroByOperatorID = new Map(
      operators
        .filter(operator => operator.macroIdParent?.trim())
        .map(operator => [operator.operatorID, operator.macroIdParent?.trim() as string])
    );
    const collapsedMacroIDs = new Set(macros.filter(macro => macro.collapsed).map(macro => macro.macroID));
    const portsByMacroID = new Map<string, joint.dia.Element.Port[]>();
    const proxyLinks: joint.dia.Link[] = [];
    const addPort = (macroID: string, direction: "in" | "out", linkID: string): string => {
      const ports = portsByMacroID.get(macroID) ?? [];
      portsByMacroID.set(macroID, ports);
      const portID = JointGraphWrapper.getMacroBoundaryPortID(direction, linkID);
      if (!ports.some(port => port.id === portID)) {
        ports.push({
          id: portID,
          group: direction,
          attrs: {
            ".port-label": {
              text: String(ports.filter(port => port.group === direction).length + 1),
            },
          },
        });
      }
      return portID;
    };

    links.forEach(link => {
      const sourceMacroID = macroByOperatorID.get(link.source.operatorID);
      const targetMacroID = macroByOperatorID.get(link.target.operatorID);
      const sourceCollapsed = sourceMacroID !== undefined && collapsedMacroIDs.has(sourceMacroID);
      const targetCollapsed = targetMacroID !== undefined && collapsedMacroIDs.has(targetMacroID);
      if ((!sourceCollapsed && !targetCollapsed) || sourceMacroID === targetMacroID) return;

      const source = sourceCollapsed
        ? {
            operatorID: JointGraphWrapper.getMacroNodeID(sourceMacroID as string),
            portID: addPort(sourceMacroID as string, "out", link.linkID),
          }
        : link.source;
      const target = targetCollapsed
        ? {
            operatorID: JointGraphWrapper.getMacroNodeID(targetMacroID as string),
            portID: addPort(targetMacroID as string, "in", link.linkID),
          }
        : link.target;
      proxyLinks.push(
        JointGraphWrapper.getMacroBoundaryLink(
          JointGraphWrapper.getMacroProxyLinkID(sourceCollapsed ? "out" : "in", link.linkID),
          source.operatorID,
          source.portID,
          target.operatorID,
          target.portID
        )
      );
    });

    macros.forEach(macro => {
      const node = this.jointGraph.getCell(JointGraphWrapper.getMacroNodeID(macro.macroID)) as
        | joint.dia.Element
        | undefined;
      if (node?.isElement()) {
        JointGraphWrapper.setMacroBoundaryPorts(node, portsByMacroID.get(macro.macroID) ?? []);
      }
    });
    this.jointGraph.addCells(proxyLinks);
  }

  private applyMacroCollapseState(operators: readonly OperatorPredicate[]): void {
    const collapsedOperatorIDs = new Set(
      operators
        .filter(operator => {
          const macroID = operator.macroIdParent?.trim();
          return macroID !== undefined && this.collapsedMacroIDs.has(macroID);
        })
        .map(operator => operator.operatorID)
    );

    operators.forEach(operator => {
      this.setCellHidden(this.jointGraph.getCell(operator.operatorID), collapsedOperatorIDs.has(operator.operatorID));
    });
    this.jointGraph.getLinks().forEach(link => {
      const sourceID = link.source()?.id;
      const targetID = link.target()?.id;
      this.setCellHidden(
        link,
        Boolean(
          (sourceID && collapsedOperatorIDs.has(sourceID.toString())) ||
            (targetID && collapsedOperatorIDs.has(targetID.toString()))
        )
      );
    });
  }

  private setCellHidden(cell: joint.dia.Cell | undefined, hidden: boolean): void {
    if (!cell) return;
    if (JointGraphWrapper.isMacroProxyLinkID(cell.id.toString())) return;
    cell.attr("root/display", hidden ? "none" : null);
    const view = this.getMainJointPaper()?.findViewByModel(cell);
    if (view) {
      view.el.style.display = hidden ? "none" : "";
    }
  }

  private refreshPaperViews(): void {
    if (!this.mainPaper) return;
    const refresh = () => {
      this.clearMacroSelectionChrome();
      this.mainPaper.updateViews();
      this.jointGraph.getLinks().forEach(link => {
        const linkView = this.mainPaper.findViewByModel(link) as
          | (joint.dia.LinkView & {
              requestConnectionUpdate?: () => void;
            })
          | null;
        linkView?.requestConnectionUpdate?.();
      });
    };
    refresh();
    if (typeof requestAnimationFrame === "function") {
      requestAnimationFrame(refresh);
    }
  }

  private clearMacroSelectionChrome(): void {
    if (!this.mainPaper) return;
    this.workflowMacros.forEach(macro => {
      const view = this.mainPaper.findViewByModel(JointGraphWrapper.getMacroNodeID(macro.macroID));
      view?.el.querySelectorAll(".joint-highlight-stroke").forEach(element => element.remove());
    });
  }

  /**
   * Highlights the link with given linkID.
   * Emits an event to the link highlight stream.
   * If the target link is already highlighted, the action will be ignored.
   * At current design, there can only be one link highlighted at a time,
   *  no mutiselect mode for links.
   * Before a link is highlighted, all the currently highlighted operators will
   *  be unhighlighted.
   *
   * @param linkID
   */
  public highlightLink(linkID: string): void {
    if (!this.jointGraph.getCell(linkID)) {
      throw new Error(`link with ID ${linkID} doesn't exist`);
    }
    if (this.currentHighlightedLinks.includes(linkID)) {
      return;
    }
    // only allow one link highlighted at a time
    if (this.currentHighlightedLinks.length > 0) {
      const highlightedLinks = Object.assign([], this.currentHighlightedLinks);
      highlightedLinks.forEach(highlightedLink => this.unhighlightLink(highlightedLink));
    }
    this.getCurrentHighlightedOperatorIDs().forEach(operatorID => this.unhighlightOperators(operatorID));
    this.currentHighlightedLinks.push(linkID);
    this.jointLinkHighlightStream.next([linkID]);
  }

  /**
   * Unhighlights the given highlighted link.
   * Emits an event to the link unhighlight stream.
   * @param linkID
   */
  public unhighlightLink(linkID: string): void {
    if (!this.currentHighlightedLinks.includes(linkID)) {
      return;
    }
    const unhighlightedLinkIndex = this.currentHighlightedLinks.indexOf(linkID);
    this.currentHighlightedLinks.splice(unhighlightedLinkIndex, 1);
    this.jointLinkUnhighlightStream.next([linkID]);
  }

  /**
   * This method gets the cell's layer (z attribute) on the JointJS paper.
   * A cell can be an operator, a link, or a group element.
   */
  public getCellLayer(cellID: string): number {
    const cell: joint.dia.Cell | undefined = this.jointGraph.getCell(cellID);
    if (!cell) {
      throw new Error(`cell with ID ${cellID} doesn't exist`);
    }
    return cell.attributes.z || 0;
  }

  /**
   * Returns the boolean value that indicates whether
   * or not listen to operator position change.
   */
  public getListenPositionChange(): boolean {
    return this.listenPositionChange;
  }

  /**
   * Sets the boolean value that indicates whether
   * or not listen to operator position change.
   */
  public setListenPositionChange(listenPositionChange: boolean): void {
    this.listenPositionChange = listenPositionChange;
  }

  private static styleMacroFrame(frame: joint.dia.Element): void {
    frame.set("type", "macro-frame");
    frame.set("z", 0);
    frame.attr({
      body: {
        fill: "rgba(47, 84, 235, 0.04)",
        stroke: "#2f54eb",
        "stroke-width": 2,
        "stroke-dasharray": "8 4",
        rx: 8,
        ry: 8,
        "pointer-events": "none",
      },
      label: { text: "" },
    });
  }

  private static getExpandedMacroTabPosition(bounds: { x: number; y: number; width: number; height: number }): Point {
    return {
      x: bounds.x + JointGraphWrapper.MACRO_NODE_INSET,
      y: bounds.y + JointGraphWrapper.MACRO_NODE_INSET,
    };
  }

  private static createMacroNode(collapsed: boolean): joint.dia.Element {
    return collapsed
      ? (new joint.shapes.devs.Model({
          markup: JointUIService.getOperatorElementMarkup(),
          ports: { groups: JointGraphWrapper.getMacroBoundaryPortGroups() },
        }) as joint.dia.Element)
      : new joint.shapes.standard.Rectangle();
  }

  private static styleMacroNode(node: joint.dia.Element, name: string, collapsed: boolean): void {
    node.set("type", "macro-node");
    node.set("z", 2);
    if (collapsed) {
      const macroOperator = {
        operatorID: node.id.toString(),
        operatorType: "WorkflowMacro",
        operatorVersion: "",
        operatorProperties: {},
        inputPorts: [],
        outputPorts: [],
        showAdvanced: false,
        customDisplayName: name,
      } as OperatorPredicate;
      node.attr(
        JointUIService.getCustomOperatorStyleAttrs(
          macroOperator,
          JointUIService.truncateOperatorDisplayName(name),
          "WorkflowMacro",
          "Workflow Macro"
        )
      );
      node.attr({
        "rect.body": { stroke: "#CFCFCF", cursor: "move" },
        ".texera-operator-icon": { cursor: "move" },
        ".texera-operator-name": { cursor: "move" },
        ".texera-operator-friendly-name": { cursor: "move" },
      });
      JointGraphWrapper.setMacroBoundaryPortGroups(node);
      return;
    }
    node.attr({
      body: {
        fill: collapsed ? "#fff7e6" : "#e6f4ff",
        stroke: collapsed ? "#fa8c16" : "#1677ff",
        "stroke-width": 2,
        rx: 8,
        ry: 8,
        cursor: "move",
      },
      label: {
        text: collapsed ? `${name}\ncollapsed` : name,
        fill: collapsed ? "#ad6800" : "#2f54eb",
        "font-size": 13,
        "font-weight": 600,
        "text-anchor": "middle",
        "x-alignment": "middle",
        "y-alignment": "middle",
        refX: 0.5,
        refY: 0.5,
        ref: "body",
        cursor: "move",
      },
    });
    JointGraphWrapper.setMacroBoundaryPortGroups(node);
  }

  private static getMacroBoundaryPortID(direction: "in" | "out", linkID: string): string {
    return `macro-${direction}-${linkID}`;
  }

  private static getMacroProxyLinkID(direction: "in" | "out", linkID: string): string {
    return `${JointGraphWrapper.MACRO_PROXY_LINK_PREFIX}${direction}-${linkID}`;
  }

  private static getMacroBoundaryLink(
    linkID: string,
    sourceOperatorID: string,
    sourcePortID: string,
    targetOperatorID: string,
    targetPortID: string
  ): joint.dia.Link {
    const link = JointUIService.getDefaultLinkCell();
    link.set("id", linkID);
    link.set("type", "macro-proxy-link");
    link.set("source", { id: sourceOperatorID, port: sourcePortID });
    link.set("target", { id: targetOperatorID, port: targetPortID });
    link.attr({
      ".connection": {
        stroke: "#919191",
        "stroke-width": "2px",
        "stroke-dasharray": "6 3",
      },
      ".connection-wrap": {
        "stroke-width": "0px",
      },
      ".tool-remove": {
        display: "none",
      },
      ".marker-source": {
        display: "none",
      },
      ".marker-arrowhead-group-source": {
        display: "none",
      },
      ".marker-arrowhead-group-target": {
        display: "none",
      },
    });
    return link;
  }

  private static setMacroBoundaryPortGroups(node: joint.dia.Element): void {
    node.set("portMarkup", '<circle class="port-body"/>');
    node.set("portLabelMarkup", '<text class="port-label"/>');
    if (!node.prop("ports/groups")) {
      node.prop("ports/groups", JointGraphWrapper.getMacroBoundaryPortGroups());
    }
  }

  private static setMacroBoundaryPorts(node: joint.dia.Element, ports: joint.dia.Element.Port[]): void {
    node.set("ports", {
      groups: JointGraphWrapper.getMacroBoundaryPortGroups(),
      items: ports,
    });
  }

  private static getMacroBoundaryPortGroups(): Record<string, joint.dia.Element.PortGroup> {
    return {
      in: JointGraphWrapper.getMacroBoundaryPortGroup("left"),
      out: JointGraphWrapper.getMacroBoundaryPortGroup("right"),
    };
  }

  private static getMacroBoundaryPortGroup(position: "left" | "right"): joint.dia.Element.PortGroup {
    return {
      position: { name: position },
      attrs: {
        ".port-body": {
          fill: "#8c8c8c",
          stroke: "#ffffff",
          "stroke-width": 1,
          r: 5,
          magnet: false,
        },
        ".port-label": {
          fill: "#595959",
          "font-size": 10,
        },
      },
      label: {
        position: {
          name: position,
          args: { y: 7 },
        },
      },
    };
  }

  /**
   * Highlights the element with given elementID.
   *
   * An element can be either an operator or a group. If the element is already
   * highlighted, the action will be ignored.
   *
   * When the multiselect mode is off:
   * there is only one element that could be highlighted at a time, therefore
   *  if there are other highlighted elements, they will be unhighlighted.
   */
  private highlightElement(
    elementID: string,
    currentHighlightedElements: string[],
    highlightedElements: string[]
  ): void {
    // try to get the element using element ID
    if (!this.jointGraph.getCell(elementID)) {
      throw new Error(`element with ID ${elementID} doesn't exist`);
    }
    // if the element is already highlighted, don't do anything
    if (currentHighlightedElements.includes(elementID)) {
      return;
    }
    // if the multiselect mode is off, unhighlight other highlighted elements first
    if (!this.multiSelect) {
      this.unhighlightOperators(...this.getCurrentHighlightedOperatorIDs());
      this.unhighlightLinks(...this.getCurrentHighlightedLinkIDs());
      this.unhighlightCommentBoxes(...this.getCurrentHighlightedCommentBoxIDs());
      this.unhighlightPorts(...this.getCurrentHighlightedPortIDs());
    }
    // highlight the element and add it to the list of highlighted elements
    currentHighlightedElements.push(elementID);
    highlightedElements.push(elementID);
  }

  /**
   * Unhighlights the given highlighted element (operator or group).
   * This function fills the unhighlightedElements array to include the unhighlighted elements.
   */
  private unhighlightElement(
    elementID: string,
    currentHighlightedElements: string[],
    unhighlightedElements: string[]
  ): void {
    if (!currentHighlightedElements.includes(elementID)) {
      return;
    }
    currentHighlightedElements.splice(currentHighlightedElements.indexOf(elementID), 1);
    unhighlightedElements.push(elementID);
  }

  /**
   * Subscribes to cell delete event stream,
   *  checks if the deleted cell (operator, link, or group) is currently highlighted
   *  and unhighlight it if it is.
   */
  private handleElementDeleteUnhighlight(): void {
    this.jointCellDeleteStream.subscribe(deletedCell => {
      const deletedCellID = deletedCell.id.toString();
      if (this.currentHighlightedOperators.includes(deletedCellID)) {
        this.unhighlightOperators(deletedCellID);
      } else if (this.currentHighlightedLinks.includes(deletedCellID)) {
        this.unhighlightLinks(deletedCellID);
      }
    });
  }

  public static jointGraphContextFactory() {
    class JointGraphContext extends ObservableContextManager<JointGraphContextType>(DefaultContext) {
      private static jointPaper: joint.dia.Paper | undefined;

      public static async() {
        return this._async(this.getContext());
      }

      // Custom RXJS operator to buffer output while the jointgraph
      // is in an async context
      public static bufferWhileAsync<T>(source: Observable<T>): Observable<T> {
        const subject = new Subject<T>();
        const buffer: T[] = [];
        const clearBuffer = () => {
          while (buffer.length > 0) {
            subject.next(buffer.pop() as T);
          }
        };

        source.subscribe({
          next: evt => {
            if (JointGraphContext.async()) {
              buffer.push(evt);
            } else {
              clearBuffer();
              subject.next(evt);
            }
          },
          error: (err: unknown) => {
            clearBuffer();
            subject.error(err);
          },
          complete: () => {
            clearBuffer();
            subject.complete();
          },
        });
        return subject;
      }

      public static attachPaper(jointPaper: joint.dia.Paper) {
        this.jointPaper = jointPaper;
        this.jointPaper.options.async = this.async();
      }

      protected static enter(context: JointGraphContextType): void {
        super.enter(context);
        if (this.jointPaper !== undefined) {
          this.jointPaper.options.async = this.async();
        }
      }

      protected static exit(): void {
        if (this.jointPaper !== undefined) {
          const CURRENT_ASYNC_MODE = this._async(this.getContext());
          const NEW_ASYNC_MODE = this._async(this.prevContext());

          this.jointPaper.options.async = NEW_ASYNC_MODE;
          if (CURRENT_ASYNC_MODE && !NEW_ASYNC_MODE) this.jointPaper.updateViews();
        }
        super.exit();
      }

      private static _async(context: JointGraphContextType) {
        return context.async;
      }
    }

    return JointGraphContext;
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //                                     Below are methods for coeditor-presence.                                     //
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  public deleteCoeditorOperatorHighlight(coeditor: Coeditor, operatorId: string) {
    const operatorElement = this.getMainJointPaper()?.findViewByModel(operatorId);
    if (operatorElement) {
      const currentStrokeIds = joint.highlighters.mask.get(operatorElement).map(stroke => stroke.id);
      const highlightIdToDelete = `coeditorHighlight_${coeditor.clientId}_${operatorId}`;
      if (currentStrokeIds.includes(highlightIdToDelete)) {
        const deletedIndex = currentStrokeIds.indexOf(highlightIdToDelete);
        joint.highlighters.mask.remove(operatorElement, highlightIdToDelete);
        currentStrokeIds.splice(deletedIndex, 1);
        const currentStrokes = joint.highlighters.mask.get(operatorElement);

        // Update other highlights on this operator to make the diameters consistent.
        for (let i = deletedIndex; i < currentStrokeIds.length; i++) {
          const previousStroke = currentStrokes[i];
          const highlightId = currentStrokeIds[i];
          if (highlightId) {
            joint.highlighters.mask.remove(operatorElement, highlightId);
            joint.highlighters.mask.add(operatorElement, "rect.body", highlightId, {
              ...previousStroke.options,
              padding: 5 + 5 * i,
            });
          }
        }
      }
    }
  }

  public addCoeditorOperatorHighlight(coeditor: Coeditor, operatorId: string) {
    const operatorElement = this.getMainJointPaper()?.findViewByModel(operatorId);
    if (operatorElement) {
      const currentStrokeIds = joint.highlighters.mask.get(operatorElement).map(stroke => stroke.id);
      const highlightId = `coeditorHighlight_${coeditor.clientId}_${operatorId}`;
      if (!currentStrokeIds.includes(highlightId)) {
        joint.highlighters.mask.add(operatorElement, "rect.body", highlightId, {
          padding: 5 + 5 * currentStrokeIds.length,
          rx: 5,
          ry: 5,
          attrs: {
            "stroke-width": 2,
            stroke: coeditor.color,
          },
        });
      }
    }
  }

  public setCurrentEditing(coeditor: Coeditor, currentEditing: string): ReturnType<typeof setInterval> {
    // Calculate location
    const statusText = coeditor.name + " is viewing/editing...";
    const color = coeditor.color;
    this.getMainJointPaper()
      ?.getModelById(currentEditing)
      .attr({
        [`.${operatorCoeditorEditingClass}`]: {
          text: statusText,
          fill: color,
          visibility: "visible",
        },
      });
    // "Animation"
    const getCurrentlyEditingText = (): string => {
      return (this.getMainJointPaper()?.getModelById(currentEditing).attributes.attrs as Selectors)[
        `.${operatorCoeditorEditingClass}`
      ]?.text as string;
    };
    return setInterval(() => {
      const currentText = getCurrentlyEditingText();
      if (currentText.includes(coeditor.name)) {
        let nextText = "";
        if (currentText.length === statusText.length) {
          nextText = coeditor.name + " is viewing/editing.";
        } else if (currentText.length === statusText.length - 1) {
          nextText = coeditor.name + " is viewing/editing...";
        } else if (currentText.length === statusText.length - 2) {
          nextText = coeditor.name + " is viewing/editing..";
        }
        this.getMainJointPaper()
          ?.getModelById(currentEditing)
          .attr({
            [`.${operatorCoeditorEditingClass}`]: {
              text: nextText,
            },
          });
      }
    }, 300);
  }

  public removeCurrentEditing(coeditor: User, previousEditing: string, intervalId: ReturnType<typeof setInterval>) {
    clearInterval(intervalId);
    this.getMainJointPaper()
      ?.getModelById(previousEditing)
      .attr({
        [`.${operatorCoeditorEditingClass}`]: {
          text: "",
          visibility: "hidden",
        },
      });
  }

  public setPropertyChanged(coeditor: User, currentChanged: string) {
    // Calculate location
    const statusText = coeditor.name + " changed property!";
    const color = coeditor.color;
    this.getMainJointPaper()
      ?.getModelById(currentChanged)
      .attr({
        [`.${operatorCoeditorChangedPropertyClass}`]: {
          text: statusText,
          fill: color,
          visibility: "visible",
        },
      });
  }

  public removePropertyChanged(coeditor: User, currentChanged: string) {
    this.getMainJointPaper()
      ?.getModelById(currentChanged)
      .attr({
        [`.${operatorCoeditorChangedPropertyClass}`]: {
          text: "",
          visibility: "hidden",
        },
      });
  }
}
