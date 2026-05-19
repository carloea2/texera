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
  it("should lock inferred fields and keep value editable", () => {
    const component = new UiUdfParametersComponent();
    const value = field("value", new FormControl({ value: "42", disabled: true }));
    const name = field("attributeName", new FormControl("threshold"));
    const type = field("attributeType", new FormControl("double"));

    (component as any).field = {
      fieldGroup: [{ fieldGroup: [value, { key: "attribute", fieldGroup: [name, type] }] }],
    };
    component.ngOnInit();

    expect([value.props?.disabled, value.formControl?.enabled]).toEqual([false, true]);
    expect([name.props?.disabled, name.formControl?.disabled]).toEqual([true, true]);
    expect([type.props?.disabled, type.formControl?.disabled]).toEqual([true, true]);
  });
});

function field(key: string, formControl: FormControl): FormlyFieldConfig {
  return { key, formControl };
}
