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
import { Component } from "@angular/core";
import { FieldArrayType, FormlyFieldConfig } from "@ngx-formly/core";

@Component({
  selector: "texera-ui-udf-parameters",
  templateUrl: "./ui-udf-parameters.component.html",
  styleUrls: ["./ui-udf-parameters.component.scss"],
})
export class UiUdfParametersComponent extends FieldArrayType {
  private getField(rowField: FormlyFieldConfig, key: string): FormlyFieldConfig | undefined {
    return rowField.fieldGroup?.find(f => f.key === key);
  }

  private getAttributeChild(rowField: FormlyFieldConfig, childKey: string): FormlyFieldConfig | undefined {
    const attributeGroup = this.getField(rowField, "attribute");
    return attributeGroup?.fieldGroup?.find(f => f.key === childKey);
  }

  private setDisabled(field: FormlyFieldConfig | undefined, disabled: boolean): FormlyFieldConfig | undefined {
    if (!field) return undefined;

    // 1) Modern Formly
    field.props = { ...(field.props ?? {}), disabled };

    // 2) Compatibility for templates/wrappers still using templateOptions
    // (`as any` so we don't get nagged by the @deprecated JSDoc)
    (field as any).templateOptions = { ...((field as any).templateOptions ?? {}), disabled };

    // 3) Enforce at the reactive form level
    if (field.formControl) {
      if (disabled) {
        field.formControl.disable({ emitEvent: false });
      } else {
        field.formControl.enable({ emitEvent: false });
      }
    } else {
      // If control isn't created yet, disable it at init time.
      const prevOnInit = field.hooks?.onInit;
      field.hooks = {
        ...(field.hooks ?? {}),
        onInit: f => {
          prevOnInit?.(f);
          if (disabled) {
            f.formControl?.disable({ emitEvent: false });
          } else {
            f.formControl?.enable({ emitEvent: false });
          }
        },
      };
    }

    return field;
  }

  // Disable Name
  getNameField(rowField: FormlyFieldConfig): FormlyFieldConfig | undefined {
    return this.setDisabled(this.getAttributeChild(rowField, "attributeName"), true);
  }

  // Disable Type
  getTypeField(rowField: FormlyFieldConfig): FormlyFieldConfig | undefined {
    return this.setDisabled(this.getAttributeChild(rowField, "attributeType"), true);
  }

  // Value editable
  getValueField(rowField: FormlyFieldConfig): FormlyFieldConfig | undefined {
    return this.setDisabled(this.getField(rowField, "value"), false);
  }

  trackByParamName = (index: number, param: any): string | number => {
    return param?.attribute?.attributeName ?? index;
  };
}
