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
    // (use `as any` so you don't get nagged by the @deprecated JSDoc)
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

  // Disable Type (set to false if you want it editable)
  getTypeField(rowField: FormlyFieldConfig): FormlyFieldConfig | undefined {
    return this.setDisabled(this.getAttributeChild(rowField, "attributeType"), true);
  }

  // Value editable (set to true to disable)
  getValueField(rowField: FormlyFieldConfig): FormlyFieldConfig | undefined {
    return this.setDisabled(this.getField(rowField, "value"), false);
  }

  trackByParamName = (index: number, param: any): string | number => {
    return param?.attribute?.attributeName ?? index;
  };
}
