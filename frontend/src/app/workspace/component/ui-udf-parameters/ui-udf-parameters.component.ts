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
import { Component, OnInit } from "@angular/core";
import { NgFor, NgIf } from "@angular/common";
import { FieldArrayType, FormlyFieldConfig, FormlyModule } from "@ngx-formly/core";

type UiUdfParameterColumn = Readonly<{ label: string; key: string; parentKey?: string; disabled: boolean }>;

@Component({
  selector: "texera-ui-udf-parameters",
  templateUrl: "./ui-udf-parameters.component.html",
  styleUrls: ["./ui-udf-parameters.component.scss"],
  imports: [NgIf, NgFor, FormlyModule],
})
export class UiUdfParametersComponent extends FieldArrayType implements OnInit {
  readonly fieldColumns: UiUdfParameterColumn[] = [
    { label: "Value", key: "value", disabled: false },
    { label: "Name", key: "attributeName", parentKey: "attribute", disabled: true },
    { label: "Type", key: "attributeType", parentKey: "attribute", disabled: true },
  ];

  ngOnInit(): void {
    this.field.fieldGroup?.forEach(rowField => {
      this.fieldColumns.forEach(column => {
        this.configureDisabledState(this.getColumnField(rowField, column), column.disabled);
      });
    });
  }

  getColumnField(rowField: FormlyFieldConfig, column: UiUdfParameterColumn): FormlyFieldConfig | undefined {
    return this.getChildField(column.parentKey ? this.getChildField(rowField, column.parentKey) : rowField, column.key);
  }

  private getChildField(rowField: FormlyFieldConfig | undefined, key: string): FormlyFieldConfig | undefined {
    return rowField?.fieldGroup?.find(fieldConfig => fieldConfig.key === key);
  }

  private configureDisabledState(field: FormlyFieldConfig | undefined, disabled: boolean): void {
    if (!field) return;

    field.props = { ...(field.props ?? {}), disabled };

    // Keep deprecated templateOptions in sync for existing Formly wrappers that still read it.
    (field as any).templateOptions = { ...((field as any).templateOptions ?? {}), disabled };

    const previousOnInit = field.hooks?.onInit;
    field.hooks = {
      ...(field.hooks ?? {}),
      onInit: initializedField => {
        previousOnInit?.(initializedField);
        this.applyDisabledState(initializedField, disabled);
      },
    };

    this.applyDisabledState(field, disabled);
  }

  private applyDisabledState(field: FormlyFieldConfig, disabled: boolean): void {
    if (disabled) field.formControl?.disable({ emitEvent: false });
    else field.formControl?.enable({ emitEvent: false });
  }

  trackByParameterName = (index: number, parameter: any): string | number => {
    return parameter?.attribute?.attributeName ?? index;
  };
}
