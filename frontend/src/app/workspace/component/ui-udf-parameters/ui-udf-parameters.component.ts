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

@Component({
  selector: "texera-ui-udf-parameters",
  templateUrl: "./ui-udf-parameters.component.html",
  styleUrls: ["./ui-udf-parameters.component.scss"],
  imports: [NgIf, NgFor, FormlyModule],
})
export class UiUdfParametersComponent extends FieldArrayType implements OnInit {
  ngOnInit(): void {
    this.field.fieldGroup?.forEach(rowField => {
      this.configureDisabledState(this.getAttributeChild(rowField, "attributeName"), true);
      this.configureDisabledState(this.getAttributeChild(rowField, "attributeType"), true);
      this.configureDisabledState(this.getField(rowField, "value"), false);
    });
  }

  private getField(rowField: FormlyFieldConfig, key: string): FormlyFieldConfig | undefined {
    return rowField.fieldGroup?.find(fieldConfig => fieldConfig.key === key);
  }

  private getAttributeChild(rowField: FormlyFieldConfig, childKey: string): FormlyFieldConfig | undefined {
    const attributeGroup = this.getField(rowField, "attribute");
    return attributeGroup?.fieldGroup?.find(fieldConfig => fieldConfig.key === childKey);
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
    disabled ? field.formControl?.disable({ emitEvent: false }) : field.formControl?.enable({ emitEvent: false });
  }

  getNameField(rowField: FormlyFieldConfig): FormlyFieldConfig | undefined {
    return this.getAttributeChild(rowField, "attributeName");
  }

  getTypeField(rowField: FormlyFieldConfig): FormlyFieldConfig | undefined {
    return this.getAttributeChild(rowField, "attributeType");
  }

  getValueField(rowField: FormlyFieldConfig): FormlyFieldConfig | undefined {
    return this.getField(rowField, "value");
  }

  trackByParameterName = (index: number, parameter: any): string | number => {
    return parameter?.attribute?.attributeName ?? index;
  };
}
