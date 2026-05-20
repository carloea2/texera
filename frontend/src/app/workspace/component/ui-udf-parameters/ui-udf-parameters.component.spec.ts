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

import { FormControl } from "@angular/forms";
import { FormlyFieldConfig } from "@ngx-formly/core";
import { UiUdfParametersComponent } from "./ui-udf-parameters.component";

describe("UiUdfParametersComponent", () => {
  let component: UiUdfParametersComponent;

  beforeEach(() => {
    component = new UiUdfParametersComponent();
  });

  it("should disable name and type fields while leaving value editable", () => {
    const valueControl = new FormControl({ value: "42", disabled: true });
    const nameControl = new FormControl("threshold");
    const typeControl = new FormControl("double");

    const valueField: FormlyFieldConfig = { key: "value", formControl: valueControl };
    const nameField: FormlyFieldConfig = { key: "attributeName", formControl: nameControl };
    const typeField: FormlyFieldConfig = { key: "attributeType", formControl: typeControl };
    const rowField: FormlyFieldConfig = {
      fieldGroup: [
        valueField,
        {
          key: "attribute",
          fieldGroup: [nameField, typeField],
        },
      ],
    };

    (component as any).field = { fieldGroup: [rowField] } as FormlyFieldConfig;

    component.ngOnInit();

    // templateOptions is deprecated, but some existing Formly wrappers still read it.
    [
      { column: component.fieldColumns[0], field: valueField, control: valueControl },
      { column: component.fieldColumns[1], field: nameField, control: nameControl },
      { column: component.fieldColumns[2], field: typeField, control: typeControl },
    ].forEach(({ column, field, control }) => {
      expect(component.getColumnField(rowField, column)).toBe(field);
      const disabled = column.disabled;
      expect((field as FormlyFieldConfig).props?.disabled).toBe(disabled);
      expect((field as any).templateOptions?.disabled).toBe(disabled);
      expect((control as FormControl).disabled).toBe(disabled);
    });
  });
});
