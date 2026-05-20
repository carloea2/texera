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
import { UiUdfParametersParseError, UiUdfParametersParserService } from "./ui-udf-parameters-parser.service";
import type { UiUdfParameter } from "./ui-udf-parameters-parser.service";
import { isDefined } from "../../../common/util/predicate";
import { isPythonUdf } from "../workflow-graph/model/workflow-graph";
import type { Text as YText } from "yjs";

const UI_PARAMETER_SYNC_DEBOUNCE_TIME_MS = 200;

@Injectable({ providedIn: "root" })
export class UiUdfParametersSyncService {
  private readonly uiParametersChangedSubject = new ReplaySubject<{ operatorId: string; parameters: UiUdfParameter[] }>(
    1
  );
  private readonly uiParametersParseErrorSubject = new ReplaySubject<{ operatorId: string; message?: string }>(1);

  readonly uiParametersChanged$ = this.uiParametersChangedSubject.asObservable();
  readonly uiParametersParseError$ = this.uiParametersParseErrorSubject.asObservable();

  constructor(
    private workflowActionService: WorkflowActionService,
    private uiUdfParametersParserService: UiUdfParametersParserService
  ) {}

  attachToYCode(operatorId: string, yCode: YText): () => void {
    const codeChanges = new Subject<string>();
    const subscription = codeChanges
      .pipe(debounceTime(UI_PARAMETER_SYNC_DEBOUNCE_TIME_MS))
      .subscribe(latestCode => this.syncStructureFromCode(operatorId, latestCode));
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
    if (!isDefined(code)) return;

    const existingParameters = (operator.operatorProperties?.uiParameters ?? []) as UiUdfParameter[];
    let mergedParameters: UiUdfParameter[];

    try {
      mergedParameters = this.buildParsedShapeWithPreservedValues(code, existingParameters);
    } catch (error) {
      if (error instanceof UiUdfParametersParseError) {
        this.uiParametersParseErrorSubject.next({ operatorId, message: error.message });
        return;
      }
      throw error;
    }

    this.uiParametersParseErrorSubject.next({ operatorId });

    if (isEqual(existingParameters, mergedParameters)) return;

    this.uiParametersChangedSubject.next({ operatorId, parameters: mergedParameters });
  }

  private buildParsedShapeWithPreservedValues(code: string, existingParameters: UiUdfParameter[]): UiUdfParameter[] {
    const parsedParameters = this.uiUdfParametersParserService.parse(code);
    const existingValues = new Map(
      existingParameters.map(parameter => [parameter.attribute.attributeName, parameter.value] as const)
    );

    return parsedParameters.map(parameter => ({
      ...parameter,
      value: existingValues.get(parameter.attribute.attributeName) ?? "",
    }));
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
