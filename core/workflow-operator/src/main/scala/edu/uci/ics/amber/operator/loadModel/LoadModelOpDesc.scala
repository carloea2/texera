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

package edu.uci.ics.amber.operator.loadModel

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.core.tuple.{AttributeType, Schema}
import edu.uci.ics.amber.core.workflow.{OutputPort, PortIdentity}
import edu.uci.ics.amber.operator.PythonOperatorDescriptor
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}

class LoadModelOpDesc extends PythonOperatorDescriptor {

  @JsonProperty(required = true)
  @JsonSchemaTitle("modelPath")
  @JsonPropertyDescription("The model to load")
  var modelPath: String = ""

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    val outputSchema = Schema()
      .add("model", AttributeType.BINARY)
    Map(operatorInfo.outputPorts.head.id -> outputSchema)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Load Model",
      "Loads a machine learning model from the specified path",
      OperatorGroupConstants.MACHINE_LEARNING_GENERAL_GROUP,
      inputPorts = List(),
      outputPorts = List(OutputPort())
    )

  override def generatePythonCode(): String = {
    s"""from pytexera import *
       |import tensorflow as tf
       |import tempfile
       |import os
       |class GenerateOperator(UDFSourceOperator):
       |
       |    @overrides
       |
       |    def produce(self) -> Iterator[Union[TupleLike, TableLike, None]]:
       |        file = DatasetFileDocument("$modelPath")
       |        bytes = file.read_file().getvalue() # return an io.Bytes object
       |
       |        with tempfile.NamedTemporaryFile(suffix='.h5', delete=False) as tmp_file:
       |            tmp_file.write(bytes)
       |            tmp_file.flush()
       |            model = tf.keras.models.load_model(tmp_file.name, compile=False)
       |            os.unlink(tmp_file.name)  # Clean up temporary file
       |
       |            yield {"model": model}
       |""".stripMargin
  }
}
