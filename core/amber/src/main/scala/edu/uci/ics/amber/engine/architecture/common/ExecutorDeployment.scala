package edu.uci.ics.amber.engine.architecture.common

import akka.actor.{Address, AddressFromURIString, Deploy}
import akka.remote.RemoteScope
import edu.uci.ics.amber.core.workflow.{
  GoToSpecificNode,
  PhysicalOp,
  PreferController,
  RoundRobinPreference
}
import edu.uci.ics.amber.engine.architecture.controller.execution.OperatorExecution
import edu.uci.ics.amber.engine.architecture.deploysemantics.AddressInfo
import edu.uci.ics.amber.engine.architecture.pythonworker.PythonWorkflowWorker
import edu.uci.ics.amber.engine.architecture.scheduling.config.OperatorConfig
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker.{
  FaultToleranceConfig,
  StateRestoreConfig,
  WorkerReplayInitialization
}
import edu.uci.ics.amber.util.VirtualIdentityUtils

object ExecutorDeployment {

  def createWorkers(
      op: PhysicalOp,
      controllerActorService: AkkaActorService,
      operatorExecution: OperatorExecution,
      operatorConfig: OperatorConfig,
      stateRestoreConfig: Option[StateRestoreConfig],
      replayLoggingConfig: Option[FaultToleranceConfig]
  ): Unit = {

    println("123321=====")

    val addressInfo = AddressInfo(
      controllerActorService.getClusterNodeAddresses,
      controllerActorService.self.path.address
    )

    operatorConfig.workerConfigs.foreach(workerConfig => {
      val workerId = workerConfig.workerId
      val workerIndex = VirtualIdentityUtils.getWorkerIndex(workerId)
      val locationPreference = op.locationPreference.getOrElse(RoundRobinPreference)
      val preferredAddress: Address = locationPreference match {
        case PreferController =>
          addressInfo.controllerAddress
        case node: GoToSpecificNode =>
          println("---------++")
          println("worker id: " + workerId)
          println("worker index: " + workerIndex)
          addressInfo.allAddresses.foreach { address =>
            println(s"Address: $address, Address host: ${address.host.getOrElse("None")}")
          }
          println("---------++")

          val targetAddress = AddressFromURIString(node.nodeAddr)

          // 尝试在所有地址中查找匹配指定节点地址的 Address
          addressInfo.allAddresses.find(addr => addr == targetAddress) match {
            case Some(address) => address
            case None =>
              throw new IllegalStateException(
                s"Designated node address '${node.nodeAddr}' not found among available addresses: " +
                  addressInfo.allAddresses.map(_.host.getOrElse("None")).mkString(", ")
              )
          }
        case RoundRobinPreference =>
          println("+++++")
          println("worker id: " + workerId)
          println("worker index: " + workerIndex)
          addressInfo.allAddresses.foreach(address =>
            println("Address: " + address + "Address host: " + address.host.get)
          )
          println("+++++")
          assert(
            addressInfo.allAddresses.nonEmpty,
            "Execution failed to start, no available computation nodes"
          )
          addressInfo.allAddresses(workerIndex % addressInfo.allAddresses.length)
      }

      val workflowWorker = if (op.isPythonBased) {
        PythonWorkflowWorker.props(workerConfig)
      } else {
        WorkflowWorker.props(
          workerConfig,
          WorkerReplayInitialization(
            stateRestoreConfig,
            replayLoggingConfig
          )
        )
      }
      // Note: At this point, we don't know if the actor is fully initialized.
      // Thus, the ActorRef returned from `controllerActorService.actorOf` is ignored.
      controllerActorService.actorOf(
        workflowWorker.withDeploy(Deploy(scope = RemoteScope(preferredAddress)))
      )
      operatorExecution.initWorkerExecution(workerId)
    })
  }

}
