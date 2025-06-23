package edu.uci.ics.amber.operator.visualization.radarPlot

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import edu.uci.ics.amber.core.tuple.{AttributeType, Schema}
import edu.uci.ics.amber.operator.PythonOperatorDescriptor
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.core.workflow.OutputPort.OutputMode
import edu.uci.ics.amber.core.workflow.{InputPort, OutputPort, PortIdentity}
import edu.uci.ics.amber.operator.metadata.annotations.{AutofillAttributeName, AutofillAttributeNameList}

@JsonSchemaInject(json = """
{
  "attributeTypeRules": {
    "selectedAttributes": {
      "enum": ["integer", "long", "double"]
    }
  }
}
""")
class RadarPlotOpDesc extends PythonOperatorDescriptor {
  @JsonProperty(value = "selectedAttributes", required = true)
  @JsonSchemaTitle("Axes")
  @JsonPropertyDescription("Numeric columns to use as radar axes")
  @AutofillAttributeNameList
  var selectedAttributes: List[String] = _

  @JsonProperty(value = "traceNameAttribute", required = false)
  @JsonSchemaTitle("Trace Name Column")
  @JsonPropertyDescription("Optional column to use for naming each radar trace")
  @AutofillAttributeName
  var traceNameAttribute: String = ""

  override def getOutputSchemas(inputSchemas: Map[PortIdentity, Schema]): Map[PortIdentity, Schema] = {
    val outputSchema = Schema()
      .add("html-content", AttributeType.STRING)
    Map(operatorInfo.outputPorts.head.id -> outputSchema)
    Map(operatorInfo.outputPorts.head.id -> outputSchema)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Radar Plot",
      "View the result in radar plot",
      OperatorGroupConstants.VISUALIZATION_BASIC_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort(mode = OutputMode.SINGLE_SNAPSHOT))
    )

  def generateRadarPlotCode(): String = {
    assert(selectedAttributes.nonEmpty)

    val attrList = selectedAttributes.map(attr => s""""$attr"""").mkString(", ")
    val traceNameCol = traceNameAttribute match {
      case null | "" => "None"
      case col       => s"'$col'"
    }

    s"""
       |        categories = [$attrList]
       |        trace_name_col = $traceNameCol
       |        max_vals = {attr: float('-inf') for attr in categories}
       |
       |        for _, row in table.iterrows():
       |            for attr in categories:
       |                max_vals[attr] = max(max_vals[attr], row[attr])
       |
       |        fig = go.Figure()
       |
       |        for _, row in table.iterrows():
       |            original_vals = []
       |            normalized_vals = []
       |            for attr in categories:
       |                original_vals.append(f"{attr}: {row[attr]}")
       |                normalized_vals.append(
       |                    row[attr] / max_vals[attr] if max_vals[attr] != 0 else 0
       |                )
       |            trace_name = row[trace_name_col] if trace_name_col is not None else ""
       |
       |            fig.add_trace(go.Scatterpolar(
       |                r=normalized_vals,
       |                theta=categories,
       |                fill='toself',
       |                name=str(trace_name) if trace_name is not None else "",
       |                text=original_vals,
       |                hoverinfo="text"
       |            ))
       |
       |        showlegend = any(row.get(trace_name_col, "") for _, row in table.iterrows()) if trace_name_col is not None else False
       |
       |        fig.update_layout(
       |            polar=dict(radialaxis=dict(visible=True)),
       |            showlegend=showlegend,
       |            width=600,
       |            height=600
       |        )
       |""".stripMargin
  }



  override def generatePythonCode(): String = {
    s"""
       |from pytexera import *
       |import plotly.graph_objects as go
       |import plotly.io
       |
       |class ProcessTableOperator(UDFTableOperator):
       |
       |    def render_error(self, error_msg):
       |        return '''<h1>Radar Plot is not available.</h1>
       |                  <p>Reason is: {} </p>
       |               '''.format(error_msg)
       |
       |    @overrides
       |    def process_table(self, table: Table, port: int):
       |        if table.empty:
       |            yield {'html-content': self.render_error("input table is empty.")}
       |            return
       |
       |        ${generateRadarPlotCode()}
       |
       |        html = plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False, config={'responsive': True})
       |        yield {'html-content': html}
       |""".stripMargin
  }
}
