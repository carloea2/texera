/*
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

package edu.uci.ics.amber.operator.visualization.radarPlot

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import edu.uci.ics.amber.core.tuple.{AttributeType, Schema}
import edu.uci.ics.amber.operator.PythonOperatorDescriptor
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.core.workflow.OutputPort.OutputMode
import edu.uci.ics.amber.core.workflow.{InputPort, OutputPort, PortIdentity}
import edu.uci.ics.amber.operator.metadata.annotations.{
  AutofillAttributeName,
  AutofillAttributeNameList
}

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

  @JsonProperty(value = "traceNameAttribute", defaultValue = "", required = false)
  @JsonSchemaTitle("Trace Name Column")
  @JsonPropertyDescription("Optional column to use for naming each radar trace")
  @AutofillAttributeName
  var traceNameAttribute: String = ""

  @JsonProperty(value = "maxNormalize", defaultValue = "true", required = true)
  @JsonSchemaTitle("Max Normalize")
  @JsonPropertyDescription("Normalize radar plot values by scaling them relative to the maximum value on their respective axes")
  var maxNormalize: Boolean = true

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    val outputSchema = Schema()
      .add("html-content", AttributeType.STRING)
    Map(operatorInfo.outputPorts.head.id -> outputSchema)
    Map(operatorInfo.outputPorts.head.id -> outputSchema)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Radar Plot",
      "View the result in a radar plot. A radar plot displays multivariate data on multiple axes arranged in a circular layout, allowing for comparison between different entities.",
      OperatorGroupConstants.VISUALIZATION_SCIENTIFIC_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort(mode = OutputMode.SINGLE_SNAPSHOT))
    )

  def generateRadarPlotCode(): String = {
    val attrList = selectedAttributes.map(attr => s""""$attr"""").mkString(", ")
    val traceNameCol = traceNameAttribute match {
      case null | "" => "None"
      case col       => s"'$col'"
    }
    val maxNormalizePython = if (maxNormalize) "True" else "False"

    s"""
       |        categories = [$attrList]
       |        if len(categories) == 0:
       |            yield {'html-content': self.render_error("No columns selected as axes.")}
       |            return
       |
       |        trace_name_col = $traceNameCol
       |        max_normalize = $maxNormalizePython
       |        max_vals = {attr: float('-inf') for attr in categories}
       |
       |        fig = go.Figure()
       |
       |        if max_normalize:
       |            for _, row in table.iterrows():
       |                for attr in categories:
       |                    max_vals[attr] = max(max_vals[attr], row[attr])
       |
       |        for _, row in table.iterrows():
       |            trace_name = row[trace_name_col] if trace_name_col is not None else ""
       |            if max_normalize:
       |                original_vals = []
       |                max_normalized_vals = []
       |                for attr in categories:
       |                    original_vals.append(f"{attr}: {row[attr]}")
       |                    max_normalized_vals.append(
       |                        row[attr] / max_vals[attr] if max_vals[attr] != 0 else 0
       |                    )
       |
       |                fig.add_trace(go.Scatterpolar(
       |                    r=max_normalized_vals,
       |                    theta=categories,
       |                    fill='toself',
       |                    name=str(trace_name) if trace_name is not None else "",
       |                    text=original_vals,
       |                    hoverinfo="text"
       |                ))
       |            else:
       |                fig.add_trace(go.Scatterpolar(
       |                    r=[row[attr] for attr in categories],
       |                    theta=categories,
       |                    fill='toself',
       |                    name=str(trace_name) if trace_name is not None else "",
       |                    text=[f"{attr}: {row[attr]}" for attr in categories],
       |                    hoverinfo="text"
       |                ))
       |
       |        showlegend = any(row.get(trace_name_col) for _, row in table.iterrows()) if trace_name_col is not None else False
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
       |            yield {'html-content': self.render_error("Input table is empty.")}
       |            return
       |
       |        ${generateRadarPlotCode()}
       |
       |        html = plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False, config={'responsive': True})
       |        yield {'html-content': html}
       |""".stripMargin
  }
}
