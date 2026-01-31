import { ChangeDetectionStrategy, Component, inject } from "@angular/core";
import { NZ_MODAL_DATA } from "ng-zorro-antd/modal";

export interface ConflictingFileModalData {
  fileName: string;
  path: string;
  size: string;
}

@Component({
  selector: "texera-conflicting-file-modal-content",
  templateUrl: "./conflicting-file-modal-content.component.html",
  styleUrls: ["./conflicting-file-modal-content.component.scss"],
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class ConflictingFileModalContentComponent {
  readonly data: ConflictingFileModalData = inject(NZ_MODAL_DATA);
}
