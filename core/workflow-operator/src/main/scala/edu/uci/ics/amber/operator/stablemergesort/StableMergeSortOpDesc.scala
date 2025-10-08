package edu.uci.ics.amber.operator.stablemergesort

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.core.executor.OpExecWithClassName
import edu.uci.ics.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.core.workflow.{InputPort, OutputPort, PhysicalOp}
import edu.uci.ics.amber.operator.LogicalOp
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.util.JSONUtils.objectMapper

import java.util
import edu.uci.ics.amber.operator.sort.SortCriteriaUnit

class StableMergeSortOpDesc extends LogicalOp {

  @JsonProperty(value = "keys", required = true)
  @JsonSchemaTitle("Sort Keys")
  @JsonPropertyDescription("List of attributes to sort by with ordering preferences")
  var keys: util.List[SortCriteriaUnit] = new util.ArrayList[SortCriteriaUnit]()

  override def getPhysicalOp(
                              workflowId: WorkflowIdentity,
                              executionId: ExecutionIdentity
                            ): PhysicalOp = {
    PhysicalOp
      .oneToOnePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithClassName(
          "edu.uci.ics.amber.operator.stablemergesort.StableMergeSortOpExec",
          objectMapper.writeValueAsString(this)
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Stable Merge Sort",
      "Stable per-partition sort with multi-key ordering (incremental run-stack merge)",
      OperatorGroupConstants.SORT_GROUP,
      List(InputPort()),
      List(OutputPort(blocking = true))
    )
}
