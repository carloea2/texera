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

import { Injectable } from "@angular/core";
import { HttpClient, HttpErrorResponse, HttpParams } from "@angular/common/http";
import { map, switchMap } from "rxjs/operators";
import { Dataset, DatasetVersion } from "../../../../common/type/dataset";
import { AppSettings } from "../../../../common/app-setting";
import { firstValueFrom, Observable } from "rxjs";
import { DashboardDataset } from "../../../type/dashboard-dataset.interface";
import { DatasetFileNode } from "../../../../common/type/datasetVersionFileTree";
import { DatasetStagedObject } from "../../../../common/type/dataset-staged-object";
import { GuiConfigService } from "../../../../common/service/gui-config.service";
import { AuthService } from "src/app/common/service/user/auth.service";

export const DATASET_BASE_URL = "dataset";
export const DATASET_CREATE_URL = DATASET_BASE_URL + "/create";
export const DATASET_UPDATE_BASE_URL = DATASET_BASE_URL + "/update";
export const DATASET_UPDATE_NAME_URL = DATASET_UPDATE_BASE_URL + "/name";
export const DATASET_UPDATE_DESCRIPTION_URL = DATASET_UPDATE_BASE_URL + "/description";
export const DATASET_UPDATE_PUBLICITY_URL = "update/publicity";
export const DATASET_UPDATE_DOWNLOADABLE_URL = "update/downloadable";
export const DATASET_LIST_URL = DATASET_BASE_URL + "/list";
export const DATASET_SEARCH_URL = DATASET_BASE_URL + "/search";
export const DATASET_DELETE_URL = DATASET_BASE_URL + "/delete";

export const DATASET_VERSION_BASE_URL = "version";
export const DATASET_VERSION_RETRIEVE_LIST_URL = DATASET_VERSION_BASE_URL + "/list";
export const DATASET_VERSION_LATEST_URL = DATASET_VERSION_BASE_URL + "/latest";
export const DEFAULT_DATASET_NAME = "Untitled dataset";
export const DATASET_PUBLIC_VERSION_BASE_URL = "publicVersion";
export const DATASET_PUBLIC_VERSION_RETRIEVE_LIST_URL = DATASET_PUBLIC_VERSION_BASE_URL + "/list";
export const DATASET_GET_OWNERS_URL = DATASET_BASE_URL + "/user-dataset-owners";

export interface MultipartUploadProgress {
  filePath: string;
  percentage: number;
  status: "initializing" | "uploading" | "finished" | "aborted" | "failed";
  uploadSpeed?: number; // bytes per second
  estimatedTimeRemaining?: number; // seconds
  totalTime?: number; // total seconds taken
  resumed?: boolean;
}

export interface MultipartUploadStatus {
  numPartsRequested: number;
  percentage: number;
  parts: number[];
}

@Injectable({
  providedIn: "root",
})
export class DatasetService {
  private static createEphemeralBrowserId(): string {
    // stable for the lifetime of the tab/app instance, not persisted
    const rnd = () => Math.floor(Math.random() * 0xffffffff).toString(16).padStart(8, "0");
    return `${Date.now().toString(16)}-${rnd()}-${rnd()}`;
  }

  private readonly browserId = DatasetService.createEphemeralBrowserId();

  constructor(private http: HttpClient, private config: GuiConfigService) {}

  public createDataset(dataset: Dataset): Observable<DashboardDataset> {
    return this.http.post<DashboardDataset>(`${AppSettings.getApiEndpoint()}/${DATASET_CREATE_URL}`, {
      datasetName: dataset.name,
      datasetDescription: dataset.description,
      isDatasetPublic: dataset.isPublic,
      isDatasetDownloadable: dataset.isDownloadable,
    });
  }

  public getDataset(did: number, isLogin: boolean = true): Observable<DashboardDataset> {
    const apiUrl = isLogin
      ? `${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/${did}`
      : `${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/public/${did}`;
    return this.http.get<DashboardDataset>(apiUrl);
  }

  /**
   * Retrieves a single file from a dataset version using a pre-signed URL.
   * @param filePath Relative file path within the dataset.
   * @param isLogin Determine whether a user is currently logged in
   * @returns Observable<Blob>
   */
  public retrieveDatasetVersionSingleFile(filePath: string, isLogin: boolean = true): Observable<Blob> {
    const endpointSegment = isLogin ? "presign-download" : "public-presign-download";
    const endpoint = `${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/${endpointSegment}?filePath=${encodeURIComponent(
      filePath
    )}`;

    return this.http
      .get<{ presignedUrl: string }>(endpoint)
      .pipe(switchMap(({ presignedUrl }) => this.http.get(presignedUrl, { responseType: "blob" })));
  }

  /**
   * Retrieves a zip file of a dataset version.
   * @param did Dataset ID
   * @param dvid (Optional) Dataset version ID. If omitted, the latest version is downloaded.
   * @returns An Observable that emits a Blob containing the zip file.
   */
  public retrieveDatasetVersionZip(did: number, dvid?: number): Observable<Blob> {
    let params = new HttpParams();

    if (dvid !== undefined && dvid !== null) {
      params = params.set("dvid", dvid.toString());
    } else {
      params = params.set("latest", "true");
    }

    return this.http.get(`${AppSettings.getApiEndpoint()}/dataset/${did}/versionZip`, {
      params,
      responseType: "blob",
    });
  }

  public retrieveAccessibleDatasets(): Observable<DashboardDataset[]> {
    return this.http.get<DashboardDataset[]>(`${AppSettings.getApiEndpoint()}/${DATASET_LIST_URL}`);
  }

  public createDatasetVersion(did: number, newVersion: string): Observable<DatasetVersion> {
    return this.http
      .post<{
        datasetVersion: DatasetVersion;
        fileNodes: DatasetFileNode[];
      }>(`${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/${did}/version/create`, newVersion, {
        headers: { "Content-Type": "text/plain" },
      })
      .pipe(
        map(response => {
          response.datasetVersion.fileNodes = response.fileNodes;
          return response.datasetVersion;
        })
      );
  }

  /**
   * Status limit is caller-provided (clamped). In multipartUpload() we compute it based on partCount.
   */
  public getMultipartUploadStatus(
    ownerEmail: string,
    datasetName: string,
    filePath: string,
    limit: number
  ): Observable<MultipartUploadStatus> {
    const params = new HttpParams()
      .set("type", "status")
      .set("ownerEmail", ownerEmail)
      .set("datasetName", datasetName)
      .set("filePath", filePath) // HttpParams encodes; don't double-encode
      .set("browserId", this.browserId)
      .set("limit", Math.max(1, Math.min(1000, limit)).toString());

    return this.http.post<MultipartUploadStatus>(
      `${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/multipart-upload`,
      {},
      { params }
    );
  }

  /**
   * Handles multipart upload for large files.
   *
   * Improvements:
   * - Reduce status call volume: remove fixed 1s poller, add shared throttle + backoff + jitter
   * - Status is called on demand (queue low/empty), and on busy/retryable only when we need more work
   * - Stall detection uses "any progress" (local part completion OR backend percentage advance)
   */
  public multipartUpload(
    ownerEmail: string,
    datasetName: string,
    filePath: string,
    file: File,
    partSize: number,
    concurrencyLimit: number
  ): Observable<MultipartUploadProgress> {
    const partCount = Math.ceil(file.size / partSize);

    // Tuning knobs
    const MAX_PART_TRIES = 6; // X tries before giving up on a part (locally)
    const QUEUE_LOW_WATER = Math.max(4, concurrencyLimit * 2);

    // Status limit depends on number of parts (and concurrency), clamped to server max (1000)
    const STATUS_BATCH_LIMIT = Math.max(
      50,
      Math.min(1000, Math.min(partCount, Math.max(200, concurrencyLimit * 50)))
    );

    // Adaptive status polling (shared across workers)
    const MIN_STATUS_INTERVAL_MS = Math.max(1500, Math.min(5000, 250 * concurrencyLimit));
    const MAX_STATUS_INTERVAL_MS = 20_000;
    const STATUS_JITTER_MS = 250;

    const STALL_GUARD_MS = 60_000; // if we can’t make progress for this long, fail

    type FailReason = "busy" | "retryable";
    type FailState = {
      attempts: number;
      lastFailMs: number;
      cooldownUntilMs: number;
      lastStatus: number;
      reason: FailReason;
    };

    const isRetryableStatus = (status: number) =>
      status === 0 || // network/CORS
      status === 408 ||
      status === 425 ||
      status === 429 ||
      status === 500 ||
      status === 502 ||
      status === 503 ||
      status === 504;

    const sleep = (ms: number) => new Promise<void>(r => setTimeout(r, ms));

    const cooldownMs = (attempts: number, reason: FailReason) => {
      // Busy should cool down shorter; retryable errors cool down longer
      const base = reason === "busy" ? 250 : 750;
      const exp = Math.min(6, Math.max(0, attempts - 1));
      const jitter = Math.floor(Math.random() * 250);
      return Math.min(30_000, base * Math.pow(2, exp) + jitter);
    };

    class FatalPartError extends Error {
      constructor(public readonly httpStatus: number, message: string) {
        super(message);
        this.name = "FatalPartError";
      }
    }

    return new Observable<MultipartUploadProgress>(observer => {
      // Progress accounting
      const inFlightLoaded = new Map<number, number>();
      let inFlightBytes = 0;
      let completedBytes = 0;

      const setInFlightLoaded = (pn: number, loaded: number) => {
        const prev = inFlightLoaded.get(pn) ?? 0;
        inFlightLoaded.set(pn, loaded);
        inFlightBytes += loaded - prev;
      };

      const clearInFlight = (pn: number) => {
        const prev = inFlightLoaded.get(pn) ?? 0;
        inFlightLoaded.delete(pn);
        inFlightBytes -= prev;
      };

      let lastProgressMs = Date.now(); // backend OR local progress

      const finalizePart = (pn: number, size: number) => {
        clearInFlight(pn);
        completedBytes += size;
        lastProgressMs = Date.now();
      };

      // State for smarter scheduling
      const failedParts = new Map<number, FailState>();
      const blacklistedParts = new Set<number>(); // gave up on this client
      const queued = new Set<number>();
      const inProgress = new Set<number>();
      const queue: number[] = [];

      let manualCancelled = false;
      let resumed = false;

      let startTime: number | null = null;
      let lastUpdateTime = 0;
      let lastETA = 0;
      const speedSamples: number[] = [];

      let backendPercentage = 0;
      let displayedPercentage = 0;

      let lastTriedPart: number | null = null;

      const activeXhrs = new Map<number, XMLHttpRequest>();

      const lastStats = {
        uploadSpeed: 0,
        estimatedTimeRemaining: 0,
        totalTime: 0,
      };

      const getTotalTime = () => (startTime ? (Date.now() - startTime) / 1000 : 0);
      const clampPct = (pct: number) => Math.max(0, Math.min(99, pct));

      const computeEffectiveUploaded = (localUploaded: number) => {
        if (file.size <= 0) return localUploaded;
        const backendUploadedApprox = (Math.max(0, Math.min(100, backendPercentage)) / 100) * file.size;
        return Math.max(localUploaded, backendUploadedApprox);
      };

      const calculateStats = (uploadedBytesForStats: number) => {
        if (startTime === null) startTime = Date.now();

        const now = Date.now();
        const elapsed = getTotalTime();

        const shouldUpdate = now - lastUpdateTime >= 1000;
        if (!shouldUpdate) {
          lastStats.totalTime = elapsed;
          return lastStats;
        }
        lastUpdateTime = now;

        const currentSpeed = elapsed > 0 ? uploadedBytesForStats / elapsed : 0;
        speedSamples.push(currentSpeed);
        if (speedSamples.length > 5) speedSamples.shift();

        const avgSpeed =
          speedSamples.length > 0 ? speedSamples.reduce((a, b) => a + b, 0) / speedSamples.length : 0;

        const remaining = file.size - uploadedBytesForStats;
        let eta = avgSpeed > 0 ? remaining / avgSpeed : 0;
        eta = Math.min(eta, 24 * 60 * 60);

        if (lastETA > 0 && eta > 0) {
          const maxChange = lastETA * 0.3;
          const diff = Math.abs(eta - lastETA);
          if (diff > maxChange) eta = lastETA + (eta > lastETA ? maxChange : -maxChange);
        }
        lastETA = eta;

        const percentComplete = file.size > 0 ? (uploadedBytesForStats / file.size) * 100 : 0;
        if (percentComplete > 95) eta = Math.min(eta, 10);

        lastStats.uploadSpeed = avgSpeed;
        lastStats.estimatedTimeRemaining = Math.max(0, Math.round(eta));
        lastStats.totalTime = elapsed;

        return lastStats;
      };

      const emitProgress = (status: MultipartUploadProgress["status"]) => {
        const localUploadedBytes = completedBytes + inFlightBytes;

        const effectiveUploaded = computeEffectiveUploaded(localUploadedBytes);
        const effectivePct = file.size > 0 ? Math.round((effectiveUploaded / file.size) * 100) : 0;

        const candidatePct = clampPct(Math.max(effectivePct, backendPercentage));
        displayedPercentage = Math.max(displayedPercentage, candidatePct);

        const stats = calculateStats(effectiveUploaded);

        observer.next({
          filePath,
          percentage: displayedPercentage,
          status,
          ...stats,
        });
      };

      const markFail = (pn: number, reason: FailReason, status: number) => {
        const now = Date.now();
        const prev = failedParts.get(pn);
        const attempts = (prev?.attempts ?? 0) + 1;

        if (attempts >= MAX_PART_TRIES) {
          blacklistedParts.add(pn);
          failedParts.delete(pn);
          return;
        }

        const cd = cooldownMs(attempts, reason);
        failedParts.set(pn, {
          attempts,
          lastFailMs: now,
          cooldownUntilMs: now + cd,
          lastStatus: status,
          reason,
        });
      };

      const isAttemptableNow = (pn: number) => {
        if (blacklistedParts.has(pn)) return false;
        const fs = failedParts.get(pn);
        if (!fs) return true;
        return Date.now() >= fs.cooldownUntilMs;
      };

      const enqueueParts = (parts: number[]) => {
        for (const pn of parts) {
          if (queued.has(pn) || inProgress.has(pn)) continue;
          if (!isAttemptableNow(pn)) continue;
          if (blacklistedParts.has(pn)) continue;

          queue.push(pn);
          queued.add(pn);
        }
      };

      const pickNextPart = (): number | undefined => {
        if (queue.length === 0) return undefined;

        if (lastTriedPart === null) {
          const pn = queue.shift()!;
          queued.delete(pn);
          lastTriedPart = pn;
          return pn;
        }

        // Avoid trying the same part twice in a row when possible
        const idx = queue.findIndex(p => p !== lastTriedPart);
        const pn = idx >= 0 ? queue.splice(idx, 1)[0] : queue.shift()!;
        queued.delete(pn);
        lastTriedPart = pn;
        return pn;
      };

      // Upload a single part once (no immediate retry here)
      const uploadPartOnce = (pn: number): Promise<"ok" | "busy" | "retryable"> => {
        const start = (pn - 1) * partSize;
        const end = Math.min(start + partSize, file.size);
        const chunk = file.slice(start, end);

        return new Promise((resolve, reject) => {
          const xhr = new XMLHttpRequest();
          activeXhrs.set(pn, xhr);

          xhr.upload.addEventListener("progress", event => {
            if (!event.lengthComputable) return;
            setInFlightLoaded(pn, event.loaded);
            emitProgress("uploading");
          });

          const cleanup = () => {
            activeXhrs.delete(pn);
          };

          xhr.addEventListener("load", () => {
            cleanup();

            // Success
            if (xhr.status === 200 || xhr.status === 204) {
              finalizePart(pn, chunk.size);
              emitProgress("uploading");
              resolve("ok");
              return;
            }

            // Busy: backend says someone else is streaming this part right now
            if (xhr.status === 409) {
              clearInFlight(pn);
              emitProgress("uploading");
              resolve("busy");
              return;
            }

            // Retryable failures: skip for now, status will re-queue later
            if (isRetryableStatus(xhr.status)) {
              clearInFlight(pn);
              emitProgress("uploading");
              resolve("retryable");
              return;
            }

            // Fatal
            clearInFlight(pn);
            emitProgress("uploading");
            reject(new FatalPartError(xhr.status, `Failed uploading part ${pn} (HTTP ${xhr.status})`));
          });

          xhr.addEventListener("error", () => {
            cleanup();
            clearInFlight(pn);
            emitProgress("uploading");
            resolve("retryable");
          });

          xhr.addEventListener("timeout", () => {
            cleanup();
            clearInFlight(pn);
            emitProgress("uploading");
            resolve("retryable");
          });

          const partUrl =
            `${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/multipart-upload/part` +
            `?ownerEmail=${encodeURIComponent(ownerEmail)}` +
            `&datasetName=${encodeURIComponent(datasetName)}` +
            `&filePath=${encodeURIComponent(filePath)}` +
            `&partNumber=${pn}`;

          xhr.open("POST", partUrl);
          xhr.setRequestHeader("Content-Type", "application/octet-stream");

          xhr.timeout = 10 * 60 * 1000;

          const token = AuthService.getAccessToken();
          if (token) xhr.setRequestHeader("Authorization", `Bearer ${token}`);

          xhr.send(chunk);
        });
      };

      // Shared throttled status fetch (one in-flight at a time across workers)
      let statusInFlight: Promise<void> | null = null;

      let nextStatusAllowedAt = 0;
      let statusCooldownMs = MIN_STATUS_INTERVAL_MS;

      const scheduleNextStatus = (extraMs: number = 0) => {
        const jitter = Math.floor(Math.random() * STATUS_JITTER_MS);
        nextStatusAllowedAt = Date.now() + statusCooldownMs + extraMs + jitter;
      };

      const fetchStatusAndRefill = async (force: boolean = false) => {
        if (statusInFlight) return statusInFlight;

        const now = Date.now();
        if (!force && now < nextStatusAllowedAt) return;

        statusInFlight = (async () => {
          try {
            const status = await firstValueFrom(
              this.getMultipartUploadStatus(ownerEmail, datasetName, filePath, STATUS_BATCH_LIMIT)
            );

            const pct = Math.max(0, Math.min(100, status.percentage || 0));
            if (pct > backendPercentage) {
              backendPercentage = pct;
              lastProgressMs = Date.now();
            }

            enqueueParts(status.parts || []);
            emitProgress("uploading");

            // Success: reset cooldown, but if we have plenty queued, delay more
            statusCooldownMs = MIN_STATUS_INTERVAL_MS;
            const extra = queue.length > QUEUE_LOW_WATER * 3 ? statusCooldownMs : 0;
            scheduleNextStatus(extra);
          } catch (e: unknown) {
            const err = e as HttpErrorResponse;

            // If another request is finalizing/aborting, do not hammer
            if (err.status === 409) {
              statusCooldownMs = Math.min(
                MAX_STATUS_INTERVAL_MS,
                Math.max(statusCooldownMs, MIN_STATUS_INTERVAL_MS) * 2
              );
              scheduleNextStatus();
              return;
            }

            // Session gone => treat as done signal
            if (err.status === 404) {
              backendPercentage = Math.max(backendPercentage, 100);
              emitProgress("uploading");
              return;
            }

            // network/5xx => back off
            if (err.status === 0 || err.status >= 500) {
              statusCooldownMs = Math.min(
                MAX_STATUS_INTERVAL_MS,
                Math.max(statusCooldownMs, MIN_STATUS_INTERVAL_MS) * 2
              );
              scheduleNextStatus();
              return;
            }

            // Other client errors => bubble
            throw err;
          } finally {
            statusInFlight = null;
          }
        })();

        return statusInFlight;
      };

      const finishUpload = async () => {
        const finishParams = new HttpParams()
          .set("type", "finish")
          .set("ownerEmail", ownerEmail)
          .set("datasetName", datasetName)
          .set("filePath", filePath);

        try {
          await firstValueFrom(
            this.http.post(`${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/multipart-upload`, {}, { params: finishParams })
          );
        } catch (e: unknown) {
          const err = e as HttpErrorResponse;
          if (err.status === 404 || err.status === 409) return;
          throw err;
        }
      };

      const initOrResume = async () => {
        const initParams = new HttpParams()
          .set("type", "init")
          .set("ownerEmail", ownerEmail)
          .set("datasetName", datasetName)
          .set("filePath", filePath)
          .set("fileSizeBytes", file.size.toString())
          .set("partSizeBytes", partSize.toString());

        try {
          await firstValueFrom(
            this.http.post(`${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/multipart-upload`, {}, { params: initParams })
          );
        } catch (e: unknown) {
          const err = e as HttpErrorResponse;
          if (err.status === 409) {
            resumed = true;
            return;
          }
          throw err;
        }
      };

      const workerLoop = async () => {
        while (!manualCancelled) {
          if (backendPercentage >= 100) return;

          // Keep queue topped up (force only when totally empty)
          if (queue.length < QUEUE_LOW_WATER) {
            await fetchStatusAndRefill(queue.length === 0);
          }

          const pn = pickNextPart();
          if (pn === undefined) {
            const stalled = Date.now() - lastProgressMs > STALL_GUARD_MS;

            if (stalled && blacklistedParts.size > 0) {
              throw new Error(
                `Stalled multipart upload. Some parts were skipped after ${MAX_PART_TRIES} tries: ` +
                `${Array.from(blacklistedParts).slice(0, 50).join(",")}` +
                (blacklistedParts.size > 50 ? "..." : "")
              );
            }

            await sleep(250);
            continue;
          }

          inProgress.add(pn);

          try {
            const res = await uploadPartOnce(pn);

            if (res === "ok") {
              failedParts.delete(pn);
              continue;
            }

            // busy or retryable: do NOT count bytes, do NOT immediately retry
            if (res === "busy") {
              markFail(pn, "busy", 409);
              if (queue.length < QUEUE_LOW_WATER) await fetchStatusAndRefill(queue.length === 0);
              continue;
            }

            // retryable
            markFail(pn, "retryable", 0);
            if (queue.length < QUEUE_LOW_WATER) await fetchStatusAndRefill(queue.length === 0);
          } finally {
            inProgress.delete(pn);
          }
        }
      };

      // Kick off async run
      (async () => {
        try {
          // Reset local state
          displayedPercentage = 0;
          backendPercentage = 0;
          completedBytes = 0;
          inFlightBytes = 0;
          inFlightLoaded.clear();
          failedParts.clear();
          blacklistedParts.clear();
          queued.clear();
          inProgress.clear();
          queue.length = 0;
          lastTriedPart = null;
          startTime = null;
          lastProgressMs = Date.now();

          observer.next({
            filePath,
            percentage: 0,
            status: "initializing",
            resumed: undefined,
            uploadSpeed: 0,
            estimatedTimeRemaining: 0,
            totalTime: 0,
          });

          await initOrResume();

          observer.next({
            filePath,
            percentage: 0,
            status: "initializing",
            resumed: resumed ? true : undefined,
            uploadSpeed: 0,
            estimatedTimeRemaining: 0,
            totalTime: 0,
          });

          // Initial fill
          await fetchStatusAndRefill(true);

          // Run workers
          const workers = Array.from({ length: Math.max(1, concurrencyLimit) }, () => workerLoop());
          await Promise.all(workers);

          // Finish
          await finishUpload();

          observer.next({
            filePath,
            percentage: 100,
            status: "finished",
            uploadSpeed: 0,
            estimatedTimeRemaining: 0,
            totalTime: getTotalTime(),
          });
          observer.complete();
        } catch (err: unknown) {
          emitProgress("failed");
          observer.error(err);
        }
      })();

      // Unsubscribe handler
      return () => {
        manualCancelled = true;
        for (const xhr of activeXhrs.values()) {
          try {
            xhr.abort();
          } catch {}
        }
        activeXhrs.clear();
      };
    });
  }

  public finalizeMultipartUpload(ownerEmail: string, datasetName: string, filePath: string, isAbort: boolean): Observable<Response> {
    const params = new HttpParams()
      .set("type", isAbort ? "abort" : "finish")
      .set("ownerEmail", ownerEmail)
      .set("datasetName", datasetName)
      .set("filePath", filePath); // HttpParams encodes; don't double-encode

    return this.http.post<Response>(`${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/multipart-upload`, {}, { params });
  }

  /**
   * Resets a dataset file difference in LakeFS.
   * @param did Dataset ID
   * @param filePath File path to reset
   */
  public resetDatasetFileDiff(did: number, filePath: string): Observable<Response> {
    const params = new HttpParams().set("filePath", encodeURIComponent(filePath));
    return this.http.put<Response>(`${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/${did}/diff`, {}, { params });
  }

  /**
   * Deletes a dataset file from LakeFS.
   * @param did Dataset ID
   * @param filePath File path to delete
   */
  public deleteDatasetFile(did: number, filePath: string): Observable<Response> {
    const params = new HttpParams().set("filePath", encodeURIComponent(filePath));
    return this.http.delete<Response>(`${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/${did}/file`, { params });
  }

  /**
   * Retrieves the list of uncommitted dataset changes (diffs).
   * @param did Dataset ID
   */
  public getDatasetDiff(did: number): Observable<DatasetStagedObject[]> {
    return this.http.get<DatasetStagedObject[]>(`${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/${did}/diff`);
  }

  /**
   * retrieve a list of versions of a dataset. The list is sorted so that the latest versions are at front.
   * @param did
   * @param isLogin
   */
  public retrieveDatasetVersionList(did: number, isLogin: boolean = true): Observable<DatasetVersion[]> {
    const apiEndPont = isLogin
      ? `${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/${did}/${DATASET_VERSION_RETRIEVE_LIST_URL}`
      : `${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/${did}/${DATASET_PUBLIC_VERSION_RETRIEVE_LIST_URL}`;
    return this.http.get<DatasetVersion[]>(apiEndPont);
  }

  /**
   * retrieve the latest version of a dataset.
   * @param did
   */
  public retrieveDatasetLatestVersion(did: number): Observable<DatasetVersion> {
    return this.http
      .get<{
        datasetVersion: DatasetVersion;
        fileNodes: DatasetFileNode[];
      }>(`${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/${did}/${DATASET_VERSION_LATEST_URL}`)
      .pipe(
        map(response => {
          response.datasetVersion.fileNodes = response.fileNodes;
          return response.datasetVersion;
        })
      );
  }

  /**
   * retrieve a list of nodes that represent the files in the version
   * @param did
   * @param dvid
   * @param isLogin
   */
  public retrieveDatasetVersionFileTree(
    did: number,
    dvid: number,
    isLogin: boolean = true
  ): Observable<{ fileNodes: DatasetFileNode[]; size: number }> {
    const apiUrl = isLogin
      ? `${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/${did}/${DATASET_VERSION_BASE_URL}/${dvid}/rootFileNodes`
      : `${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/${did}/${DATASET_PUBLIC_VERSION_BASE_URL}/${dvid}/rootFileNodes`;
    return this.http.get<{ fileNodes: DatasetFileNode[]; size: number }>(apiUrl);
  }

  public deleteDatasets(did: number): Observable<Response> {
    return this.http.delete<Response>(`${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/${did}`);
  }

  public updateDatasetName(did: number, name: string): Observable<Response> {
    return this.http.post<Response>(`${AppSettings.getApiEndpoint()}/${DATASET_UPDATE_NAME_URL}`, {
      did: did,
      name: name,
    });
  }

  public updateDatasetDescription(did: number, description: string): Observable<Response> {
    return this.http.post<Response>(`${AppSettings.getApiEndpoint()}/${DATASET_UPDATE_DESCRIPTION_URL}`, {
      did: did,
      description: description,
    });
  }

  public updateDatasetPublicity(did: number): Observable<Response> {
    return this.http.post<Response>(`${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/${did}/${DATASET_UPDATE_PUBLICITY_URL}`, {});
  }

  public updateDatasetDownloadable(did: number): Observable<Response> {
    return this.http.post<Response>(
      `${AppSettings.getApiEndpoint()}/${DATASET_BASE_URL}/${did}/${DATASET_UPDATE_DOWNLOADABLE_URL}`,
      {}
    );
  }

  public retrieveOwners(): Observable<string[]> {
    return this.http.get<string[]>(`${AppSettings.getApiEndpoint()}/${DATASET_GET_OWNERS_URL}`);
  }

  public updateDatasetCoverImage(did: number, coverImage: string): Observable<Response> {
    return this.http.post<Response>(`${AppSettings.getApiEndpoint()}/dataset/${did}/update/cover`, {
      coverImage: coverImage,
    });
  }
}
