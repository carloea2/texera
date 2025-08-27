/**
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

import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { Component, EventEmitter, Input, Output } from "@angular/core";
import { ShareAccessComponent } from "../../share-access/share-access.component";
import { NotificationService } from "../../../../../common/service/notification/notification.service";
import { NzModalService } from "ng-zorro-antd/modal";
import { DASHBOARD_USER_DATASET } from "../../../../../app-routing.constant";
import { DashboardModel } from "../../../../type/dashboard-model.interface";
import { Model } from "../../../../../common/type/model";
import { ModelService } from "../../../../service/user/model/model.service";

@UntilDestroy()
@Component({
  selector: "texera-user-model-list-item",
  templateUrl: "./user-model-list-item.component.html",
  styleUrls: ["./user-model-list-item.component.scss"],
})
export class UserModelListItemComponent {
  protected readonly DASHBOARD_USER_DATASET = DASHBOARD_USER_DATASET;

  private _entry?: DashboardModel;

  @Output()
  refresh = new EventEmitter<void>();

  @Input()
  get entry(): DashboardModel {
    if (!this._entry) {
      throw new Error("entry property must be provided to UserDatasetListItemComponent.");
    }
    return this._entry;
  }

  set entry(value: DashboardModel) {
    this._entry = value;
  }

  get model(): Model {
    if (!this.entry.model) {
      throw new Error(
        "Incorrect type of DashboardEntry provided to UserModelListItemComponent. Entry must be model.",
      );
    }
    return this.entry.model;
  }

  @Input() editable = false;
  @Output() deleted = new EventEmitter<void>();
  @Output() duplicated = new EventEmitter<void>();

  editingName = false;
  editingDescription = false;

  constructor(
    private modalService: NzModalService,
    private modelService: ModelService,
    private notificationService: NotificationService,
  ) {
  }

  public confirmUpdateDatasetCustomName(name: string) {
    if (this.entry.model.name === name) {
      return;
    }

    if (this.entry.model.mid)
      this.modelService
        .updateModelName(this.entry.model.mid, name)
        .pipe(untilDestroyed(this))
        .subscribe({
          next: () => {
            this.entry.model.name = name;
            this.editingName = false;
          },
          error: () => {
            this.notificationService.error("Update model name failed");
            this.editingName = false;
          },
        });
  }

  public confirmUpdateDatasetCustomDescription(description: string) {
    if (this.entry.model.description === description) {
      return;
    }

    if (this.entry.model.mid)
      this.modelService
        .updateModelDescription(this.entry.model.mid, description)
        .pipe(untilDestroyed(this))
        .subscribe({
          next: () => {
            this.entry.model.description = description;
            this.editingDescription = false;
          },
          error: () => {
            this.notificationService.error("Update model description failed");
            this.editingDescription = false;
          },
        });
  }

  public onClickOpenShareAccess() {
    this.modalService.create({
      nzContent: ShareAccessComponent,
      nzData: {
        writeAccess: this.entry.accessPrivilege === "WRITE",
        type: "model",
        id: this.model.mid,
      },
      nzFooter: null,
      nzTitle: "Share this model with others",
      nzCentered: true,
    });
  }
}
