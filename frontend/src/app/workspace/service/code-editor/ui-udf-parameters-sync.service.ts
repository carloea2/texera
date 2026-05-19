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
import { Injectable } from "@angular/core";
import { isEqual } from "lodash-es";
import { ReplaySubject, Subject } from "rxjs";
import { debounceTime } from "rxjs/operators";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { UiUdfParametersParserService } from "./ui-udf-parameters-parser.service";
import { isPythonUdf } from "../workflow-graph/model/workflow-graph";
import type { Text as YText } from "yjs";
import { UiUdfParameter } from "../../types/workflow-compiling.interface";

type UiParametersChanged = Readonly<{ operatorId: string; parameters: UiUdfParameter[] }>;

@Injectable({ providedIn: "root" })
export class UiUdfParametersSyncService {
  private readonly uiParametersChangedSubject = new ReplaySubject<UiParametersChanged>(1);

  readonly uiParametersChanged$ = this.uiParametersChangedSubject.asObservable();

  constructor(
    private workflowActionService: WorkflowActionService,
    private uiUdfParametersParserService: UiUdfParametersParserService
  ) {}

  attachToYCode(operatorId: string, yCode: YText): () => void {
    const codeChanges = new Subject<string>();
    const subscription = codeChanges
      .pipe(debounceTime(200))
      .subscribe(code => this.syncStructureFromCode(operatorId, code));
    const handler = () => codeChanges.next(yCode.toString());

    yCode.observe(handler);
    this.syncStructureFromCode(operatorId, yCode.toString());

    return () => {
      yCode.unobserve(handler);
      subscription.unsubscribe();
      codeChanges.complete();
    };
  }

  syncStructureFromCode(operatorId: string, codeFromEditor?: string): void {
    const operator = this.workflowActionService.getTexeraGraph().getOperator(operatorId);

    if (!operator || !isPythonUdf(operator)) return;

    const code = codeFromEditor ?? this.getSharedCode(operatorId);
    if (code === undefined) return;

    const existingParameters = (operator.operatorProperties?.uiParameters ?? []) as UiUdfParameter[];
    const existingValues = new Map(
      existingParameters.map(parameter => [parameter.attribute.attributeName, parameter.value])
    );
    const mergedUiParameters = this.uiUdfParametersParserService.parse(code).map(parameter => ({
      ...parameter,
      value: existingValues.get(parameter.attribute.attributeName) ?? "",
    }));

    if (isEqual(existingParameters, mergedUiParameters)) return;

    this.uiParametersChangedSubject.next({ operatorId, parameters: mergedUiParameters });
  }

  private getSharedCode(operatorId: string): string | undefined {
    try {
      const sharedOperatorType = this.workflowActionService.getTexeraGraph().getSharedOperatorType(operatorId);
      const operatorProperties = sharedOperatorType.get("operatorProperties") as any;
      const yCode = operatorProperties.get("code") as YText;
      return yCode?.toString();
    } catch {
      return undefined;
    }
  }
}
