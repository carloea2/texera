package org.apache.texera.amber.operator.machineLearning.sklearnAdvanced.base

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations._
import org.apache.texera.amber.operator.metadata.annotations.{
  CommonOpDescAnnotation,
  HideAnnotation
}

class HyperParameters[T] {

  @JsonProperty(required = true)
  @JsonSchemaTitle("Parameter")
  @JsonPropertyDescription("Choose the name of the parameter")
  var parameter: T = _

  @JsonSchemaInject(
    strings = Array(
      new JsonSchemaString(
        path = CommonOpDescAnnotation.autofill,
        value = CommonOpDescAnnotation.attributeName
      ),
      new JsonSchemaString(path = HideAnnotation.hideTarget, value = "parametersSource"),
      new JsonSchemaString(path = HideAnnotation.hideType, value = HideAnnotation.Type.`equals`),
      new JsonSchemaString(path = HideAnnotation.hideExpectedValue, value = "false")
    ),
    ints = Array(
      new JsonSchemaInt(path = CommonOpDescAnnotation.autofillAttributeOnPort, value = 1)
    )
  )
  @JsonProperty(value = "attribute")
  var attribute: String = _

  @JsonSchemaInject(
    strings = Array(
      new JsonSchemaString(path = HideAnnotation.hideTarget, value = "parametersSource"),
      new JsonSchemaString(path = HideAnnotation.hideType, value = HideAnnotation.Type.`equals`),
      new JsonSchemaString(path = HideAnnotation.hideExpectedValue, value = "true")
    ),
    bools = Array(new JsonSchemaBool(path = HideAnnotation.hideOnNull, value = true))
  )
  @JsonProperty(value = "value")
  var value: String = _

  @JsonProperty(defaultValue = "false")
  @JsonSchemaTitle("Workflow")
  @JsonPropertyDescription("Parameter from workflow")
  var parametersSource: Boolean = false
}
