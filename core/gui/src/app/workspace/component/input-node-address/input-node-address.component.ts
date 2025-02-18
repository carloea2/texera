import { Component, OnInit, OnDestroy } from "@angular/core";
import { FieldType, FieldTypeConfig } from "@ngx-formly/core";
import { Subscription } from "rxjs";
import { WorkflowWebsocketService } from "../../service/workflow-websocket/workflow-websocket.service";

@Component({
  selector: "texera-input-node-address",
  templateUrl: "./input-node-address.component.html",
  styleUrls: ["./input-node-address.component.scss"],
})
export class InputNodeAddressComponent extends FieldType<FieldTypeConfig> implements OnInit, OnDestroy {
  // 用于存储当前的节点地址列表
  public nodeAddresses: string[] = [];

  // 订阅服务事件的 Subscription
  private wsSubscription!: Subscription;

  constructor(private workflowWebsocketService: WorkflowWebsocketService) {
    super();
  }

  ngOnInit(): void {
    // 组件初始化时先取一次当前的地址列表
    this.nodeAddresses = this.workflowWebsocketService.workerAddresses;

    // 订阅 websocket 事件，实时更新节点地址
    this.wsSubscription = this.workflowWebsocketService.websocketEvent().subscribe(event => {
      if (event.type === "ClusterStatusUpdateEvent") {
        // 更新地址列表（后端事件中包含 addresses 字段）
        this.nodeAddresses = event.addresses;
      }
    });
  }

  ngOnDestroy(): void {
    if (this.wsSubscription) {
      this.wsSubscription.unsubscribe();
    }
  }
}
