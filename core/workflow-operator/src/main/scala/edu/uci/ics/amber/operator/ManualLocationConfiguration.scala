package edu.uci.ics.amber.operator

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaString, JsonSchemaTitle}
import edu.uci.ics.amber.operator.metadata.annotations.{HideAnnotation, UIWidget}
import edu.uci.ics.amber.core.workflow.{GoToSpecificNode, PhysicalOp}

/**
 * Provides configuration for manually specifying the node location.
 * When `manual` is true and `nodeAddr` is not null, the specified node address will be used.
 */
trait ManualLocationConfiguration {

  @JsonProperty(defaultValue = "false")
  @JsonSchemaTitle("Manual Location")
  @JsonPropertyDescription("Set to true to manually specify the node address instead of using the default RoundRobin strategy")
  var manualLocation: Boolean = false

  //  @JsonProperty(required = false)
  //  @JsonSchemaTitle("Designated Node Address")
  //  @JsonSchemaInject(json = UIWidget.UIWidgetTextArea)
  //  @JsonPropertyDescription("The node address to use when manual location is enabled")
  //  var nodeAddr: String = _

  @JsonProperty(required = false)
  @JsonSchemaTitle("Designated Node Address")
  @JsonPropertyDescription("The node address to use when manual location is enabled")
  @JsonSchemaInject(
    strings = Array(
      // This configuration hides `nodeAddr` when `manual` equals "false".
      new JsonSchemaString(path = HideAnnotation.hideTarget, value = "manualLocation"),
      new JsonSchemaString(path = HideAnnotation.hideType, value = HideAnnotation.Type.equals),
      new JsonSchemaString(path = HideAnnotation.hideExpectedValue, value = "false")
    )
  )
  var nodeAddr: String = _

  /**
   * Applies the manual location configuration to the given PhysicalOp.
   *
   * @param baseOp The base PhysicalOp object
   * @return The PhysicalOp object with the applied location preference if manual is enabled; otherwise, the original baseOp.
   */
  def applyManualLocation(baseOp: PhysicalOp): PhysicalOp = {
    if (manualLocation && nodeAddr != null) {
      baseOp.withLocationPreference(Some(GoToSpecificNode(nodeAddr)))
    } else {
      baseOp
    }
  }
}
