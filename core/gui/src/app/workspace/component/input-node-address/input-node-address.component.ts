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
  public nodeAddresses: string[] = [];

  private wsSubscription!: Subscription;

  constructor(private workflowWebsocketService: WorkflowWebsocketService) {
    super();
  }

  ngOnInit(): void {
    this.nodeAddresses = this.workflowWebsocketService.workerAddresses;

    this.wsSubscription = this.workflowWebsocketService.websocketEvent().subscribe(event => {
      if (event.type === "ClusterStatusUpdateEvent") {
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
