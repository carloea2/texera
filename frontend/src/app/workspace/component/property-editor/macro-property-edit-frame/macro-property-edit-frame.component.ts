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

import { Component, Input, OnChanges, OnDestroy, OnInit } from "@angular/core";
import { FormsModule } from "@angular/forms";
import { NgFor, NgIf } from "@angular/common";
import { NzOptionComponent, NzSelectComponent } from "ng-zorro-antd/select";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { Subject } from "rxjs";
import { finalize, take, takeUntil } from "rxjs/operators";
import { WorkflowPersistService } from "../../../../common/service/workflow-persist/workflow-persist.service";
import { DashboardWorkflow } from "../../../../dashboard/type/dashboard-workflow.interface";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { JointGraphWrapper } from "../../../service/workflow-graph/model/joint-graph-wrapper";
import { WorkflowMacro } from "../../../types/workflow-common.interface";

@Component({
  selector: "texera-macro-property-edit-frame",
  templateUrl: "./macro-property-edit-frame.component.html",
  styleUrls: ["./macro-property-edit-frame.component.scss"],
  imports: [FormsModule, NgFor, NgIf, NzSelectComponent, NzOptionComponent, NzButtonComponent, NzIconDirective],
})
export class MacroPropertyEditFrameComponent implements OnInit, OnChanges, OnDestroy {
  @Input() macroNodeId?: string;

  public workflows: DashboardWorkflow[] = [];
  public selectedWorkflowId?: number;
  public loading = false;
  public canModify = true;
  private macroID?: string;
  private destroy$ = new Subject<void>();

  constructor(
    private workflowPersistService: WorkflowPersistService,
    private workflowActionService: WorkflowActionService,
    private notificationService: NotificationService
  ) {}

  ngOnInit(): void {
    this.canModify = this.workflowActionService.checkWorkflowModificationEnabled();
    this.workflowActionService
      .getWorkflowModificationEnabledStream()
      .pipe(takeUntil(this.destroy$))
      .subscribe(canModify => (this.canModify = canModify));

    this.workflowPersistService
      .retrieveWorkflowsBySessionUser()
      .pipe(take(1))
      .subscribe(workflows => (this.workflows = workflows));
  }

  ngOnChanges(): void {
    if (!this.macroNodeId) return;
    this.macroID = JointGraphWrapper.getMacroIDFromNodeID(this.macroNodeId);
    this.selectedWorkflowId = this.macro?.workflowId;
  }

  public get macro(): WorkflowMacro | undefined {
    return this.macroID ? this.workflowActionService.getWorkflowMacro(this.macroID) : undefined;
  }

  public onWorkflowChange(workflowId: number | undefined): void {
    if (!this.canModify || !this.macroID || workflowId === undefined) return;
    this.loading = true;
    this.workflowPersistService
      .retrieveWorkflow(workflowId)
      .pipe(
        take(1),
        finalize(() => (this.loading = false))
      )
      .subscribe({
        next: workflow => {
          this.workflowActionService.replaceMacroWorkflow(this.macroID!, workflow.content, workflow.wid, workflow.name);
          this.notificationService.info(`Imported workflow "${workflow.name}" into macro.`);
        },
        error: () => this.notificationService.error("Failed to import workflow into macro."),
      });
  }

  public toggleCollapsed(): void {
    if (!this.canModify || !this.macroID || !this.macro) return;
    this.workflowActionService.setMacroCollapsed(this.macroID, !(this.macro.collapsed ?? false));
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }
}
