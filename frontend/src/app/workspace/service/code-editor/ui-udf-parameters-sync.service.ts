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
import { ReplaySubject } from "rxjs";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { UiUdfParameter, UiUdfParametersParserService } from "./ui-udf-parameters-parser.service";
import { isDefined } from "../../../common/util/predicate";
import {
  DUAL_INPUT_PORTS_PYTHON_UDF_V2_OP_TYPE,
  PYTHON_UDF_SOURCE_V2_OP_TYPE,
  PYTHON_UDF_V2_OP_TYPE,
} from "../workflow-graph/model/workflow-graph";
import { YType } from "../../types/shared-editing.interface";
import type { Text as YText } from "yjs";

@Injectable({ providedIn: "root" })
export class UiUdfParametersSyncService {
  private readonly uiParametersChangedSubject = new ReplaySubject<{ operatorId: string; parameters: UiUdfParameter[] }>(
    1
  );

  readonly uiParametersChanged$ = this.uiParametersChangedSubject.asObservable();

  constructor(
    private workflowActionService: WorkflowActionService,
    private uiUdfParametersParserService: UiUdfParametersParserService
  ) {}

  /**
   * Attach directly to YText and sync whenever it changes
   */
  attachToYCode(operatorId: string, yCode: YText): () => void {
    const handler = () => {
      const latestCode = yCode.toString();
      this.syncStructureFromCode(operatorId, latestCode);
    };

    yCode.observe(handler);

    handler();

    // return cleanup function
    return () => yCode.unobserve(handler);
  }

  syncStructureFromCode(operatorId: string, codeFromEditor?: string): void {
    const operator = this.workflowActionService.getTexeraGraph().getOperator(operatorId);

    if (!operator || !this.isSupportedPythonUdfType(operator.operatorType)) {
      return;
    }

    const code = codeFromEditor ?? this.getSharedCode(operatorId);
    if (!isDefined(code)) {
      return;
    }

    const existingParameters = operator.operatorProperties?.uiParameters ?? [];
    const mergedUiParameters = this.buildParsedShapeWithPreservedValues(code, existingParameters);

    if (isEqual(existingParameters, mergedUiParameters)) {
      return;
    }

    // Emit event so UI updates
    this.uiParametersChangedSubject.next({
      operatorId,
      parameters: mergedUiParameters,
    });
  }

  private buildParsedShapeWithPreservedValues(code: string, existingParameters: any[]): UiUdfParameter[] {
    const parsedParameters = this.uiUdfParametersParserService.parse(code);

    const existingValues = new Map<string, string>();
    existingParameters.forEach((parameter: any) => {
      const parameterName = parameter?.attribute?.attributeName ?? parameter?.attribute?.name;

      if (isDefined(parameterName) && isDefined(parameter?.value)) {
        existingValues.set(parameterName, parameter.value);
      }
    });

    return parsedParameters.map(parameter => ({
      ...parameter,
      value: existingValues.get(parameter.attribute.attributeName) ?? "",
    }));
  }

  private getSharedCode(operatorId: string): string | undefined {
    try {
      const sharedOperatorType = this.workflowActionService.getTexeraGraph().getSharedOperatorType(operatorId);

      const operatorProperties = sharedOperatorType.get("operatorProperties") as YType<
        Readonly<{ [key: string]: any }>
      >;

      const yCode = operatorProperties.get("code") as YText;
      return yCode?.toString();
    } catch {
      return undefined;
    }
  }

  private isSupportedPythonUdfType(operatorType: string): boolean {
    return [PYTHON_UDF_V2_OP_TYPE, PYTHON_UDF_SOURCE_V2_OP_TYPE, DUAL_INPUT_PORTS_PYTHON_UDF_V2_OP_TYPE].includes(
      operatorType
    );
  }
}
