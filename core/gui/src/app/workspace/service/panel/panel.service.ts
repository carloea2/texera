import { Subject } from "rxjs";
import { Injectable } from "@angular/core";

@Injectable({
  providedIn: "root",
})
export class PanelService {
  private closePanelSubject = new Subject<void>();
  private resetPanelSubject = new Subject<void>();
  private openPropertyPanelSubject = new Subject<void>();
  private closePropertyPanelSubject = new Subject<void>();

  private _isPanelOpen = false;

  get isPanelOpen(): boolean {
    return this._isPanelOpen;
  }

  get resetPanelStream() {
    this._isPanelOpen = true;
    return this.resetPanelSubject.asObservable();
  }

  resetPanels() {
    this._isPanelOpen = true;
    this.resetPanelSubject.next();
  }

  get closePanelStream() {
    this._isPanelOpen = false;
    return this.closePanelSubject.asObservable();
  }

  closePanels() {
    this._isPanelOpen = false;
    this.closePanelSubject.next();
  }

  get openPropertyPanelStream() {
    this._isPanelOpen = true;
    return this.openPropertyPanelSubject.asObservable();
  }

  openPropertyPanel() {
    this._isPanelOpen = true;
    this.openPropertyPanelSubject.next();
  }

  get closePropertyPanelStream() {
    this._isPanelOpen = false;
    return this.closePropertyPanelSubject.asObservable();
  }

  closePropertyPanel() {
    this._isPanelOpen = false;
    this.closePropertyPanelSubject.next();
  }
}
