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

import { Component, inject, OnInit } from "@angular/core";
import { NZ_MODAL_DATA, NzModalRef } from "ng-zorro-antd/modal";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { DatasetFileNode } from "../../../common/type/datasetVersionFileTree";

import { DashboardModel } from "../../../dashboard/type/dashboard-model.interface";
import { ModelService } from "../../../dashboard/service/user/model/model.service";
import { parseFilePathToDatasetFile } from "../../../common/type/dataset-file";
import { ModelVersion } from "../../../common/type/model";

@UntilDestroy()
@Component({
  selector: "texera-model-selection-model",
  templateUrl: "model-selection.component.html",
  styleUrls: ["model-selection.component.scss"],
})
export class ModelSelectionComponent implements OnInit {
  readonly selectedFilePath: string = inject(NZ_MODAL_DATA).selectedFilePath;
  private _models: ReadonlyArray<DashboardModel> = [];

  // indicate whether the accessible datasets have been loaded from the backend
  isAccessibleModelsLoading = true;

  selectedModel?: DashboardModel;
  selectedVersion?: ModelVersion;
  modelVersions?: ModelVersion[];
  suggestedFileTreeNodes: DatasetFileNode[] = [];
  isModelSelected: boolean = false;

  constructor(
    private modalRef: NzModalRef,
    private modelService: ModelService
  ) {}

  ngOnInit() {
    this.isAccessibleModelsLoading = true;

    // retrieve all the accessible models from the backend
    this.modelService
      .retrieveAccessibleModels()
      .pipe(untilDestroyed(this))
      .subscribe(models => {
        this._models = models;
        this.isAccessibleModelsLoading = false;
        if (!this.selectedFilePath || this.selectedFilePath == "") {
          return;
        }
        // if users already select some file, then ONLY show that selected dataset & related version
        const selectedDatasetFile = parseFilePathToDatasetFile(this.selectedFilePath);
        this.selectedModel = this.models.find(
          d => d.ownerEmail === selectedDatasetFile.ownerEmail && d.model.name === selectedDatasetFile.datasetName
        );
        this.isModelSelected = !!this.selectedModel;
        if (this.selectedModel && this.selectedModel.model.mid !== undefined) {
          this.modelService
            .retrieveModelVersionList(this.selectedModel.model.mid)
            .pipe(untilDestroyed(this))
            .subscribe(versions => {
              this.modelVersions = versions;
              this.selectedVersion = this.modelVersions.find(v => v.name === selectedDatasetFile.versionName);
              this.onVersionChange();
            });
        }
      });
  }

  onDatasetChange() {
    this.selectedVersion = undefined;
    this.suggestedFileTreeNodes = [];
    this.isModelSelected = !!this.selectedModel;
    if (this.selectedModel && this.selectedModel.model.mid !== undefined) {
      this.modelService
        .retrieveModelVersionList(this.selectedModel.model.mid)
        .pipe(untilDestroyed(this))
        .subscribe(versions => {
          this.modelVersions = versions;
          if (this.modelVersions && this.modelVersions.length > 0) {
            this.selectedVersion = this.modelVersions[0];
            this.onVersionChange();
          }
        });
    }
  }

  onVersionChange() {
    this.suggestedFileTreeNodes = [];
    if (
      this.selectedModel &&
      this.selectedModel.model.mid !== undefined &&
      this.selectedVersion &&
      this.selectedVersion.mvid !== undefined
    ) {
      this.modelService
        .retrieveModelVersionFileTree(this.selectedModel.model.mid, this.selectedVersion.mvid)
        .pipe(untilDestroyed(this))
        .subscribe(data => {
          this.suggestedFileTreeNodes = data.fileNodes;
        });
    }
  }

  onFileTreeNodeSelected(node: DatasetFileNode) {
    this.modalRef.close(node);
  }

  get models(): ReadonlyArray<DashboardModel> {
    return this._models;
  }
}
