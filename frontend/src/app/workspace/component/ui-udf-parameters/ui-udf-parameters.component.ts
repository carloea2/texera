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
import { NgFor } from "@angular/common";
import { Component, OnInit } from "@angular/core";
import { FieldArrayType, FormlyFieldConfig, FormlyModule } from "@ngx-formly/core";

const COLUMN_KEYS = ["value", "attributeName", "attributeType"] as const;

@Component({
  selector: "texera-ui-udf-parameters",
  template:
    '<div class="ui-udf-param-row" *ngFor="let rowField of field.fieldGroup"><formly-field *ngFor="let childField of displayFields(rowField)" [field]="childField"></formly-field></div>',
  styles: [
    ".ui-udf-param-row{display:grid;grid-template-columns:minmax(160px,250px) minmax(160px,250px) minmax(120px,1fr);gap:12px}:host ::ng-deep .ant-form-item{margin-bottom:0}:host ::ng-deep .ant-form-item-label{display:none}",
  ],
  imports: [NgFor, FormlyModule],
})
export class UiUdfParametersComponent extends FieldArrayType implements OnInit {
  ngOnInit(): void {
    this.field.fieldGroup?.forEach(rowField =>
      COLUMN_KEYS.forEach(key => this.setDisabled(this.child(rowField, key), key !== "value"))
    );
  }

  displayFields(rowField: FormlyFieldConfig): FormlyFieldConfig[] {
    return COLUMN_KEYS.map(key => this.child(rowField, key)).filter((field): field is FormlyFieldConfig => !!field);
  }

  private child(rowField: FormlyFieldConfig, key: string): FormlyFieldConfig | undefined {
    if (key === "value") return rowField.fieldGroup?.find(field => field.key === key);
    return rowField.fieldGroup?.find(field => field.key === "attribute")?.fieldGroup?.find(field => field.key === key);
  }

  private setDisabled(field: FormlyFieldConfig | undefined, disabled: boolean): void {
    if (!field) return;
    field.props = { ...(field.props ?? {}), disabled };
    field.formControl?.[disabled ? "disable" : "enable"]({ emitEvent: false });
  }
}
