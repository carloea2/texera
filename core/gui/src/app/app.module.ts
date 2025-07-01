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

import { DatePipe, registerLocaleData } from "@angular/common";
import { HTTP_INTERCEPTORS, HttpClientModule } from "@angular/common/http";
import en from "@angular/common/locales/en";
import { NgModule } from "@angular/core";
import { FormsModule, ReactiveFormsModule } from "@angular/forms";
import { BrowserModule } from "@angular/platform-browser";
import { BrowserAnimationsModule } from "@angular/platform-browser/animations";
import { RouterModule } from "@angular/router";
import { FormlyModule } from "@ngx-formly/core";
import { NzButtonModule } from "ng-zorro-antd/button";
import { NzCollapseModule } from "ng-zorro-antd/collapse";
import { NzDatePickerModule } from "ng-zorro-antd/date-picker";
import { NzDropDownModule } from "ng-zorro-antd/dropdown";
import { NzFormModule } from "ng-zorro-antd/form";
import { NzAutocompleteModule } from "ng-zorro-antd/auto-complete";
import { NzIconModule } from "ng-zorro-antd/icon";
import { NzInputModule } from "ng-zorro-antd/input";
import { NzPopoverModule } from "ng-zorro-antd/popover";
import { NzListModule } from "ng-zorro-antd/list";
import { NzTableModule } from "ng-zorro-antd/table";
import { NzToolTipModule } from "ng-zorro-antd/tooltip";
import { NzSelectModule } from "ng-zorro-antd/select";
import { NzSpaceModule } from "ng-zorro-antd/space";
import { NzBadgeModule } from "ng-zorro-antd/badge";
import { NzUploadModule } from "ng-zorro-antd/upload";
import { NgxJsonViewerModule } from "ngx-json-viewer";
import { ColorPickerModule } from "ngx-color-picker";
import { AppRoutingModule } from "./app-routing.module";
import { AppComponent } from "./app.component";
import { ArrayTypeComponent } from "./common/formly/array.type";
import { TEXERA_FORMLY_CONFIG } from "./common/formly/formly-config";
import { MultiSchemaTypeComponent } from "./common/formly/multischema.type";
import { NullTypeComponent } from "./common/formly/null.type";
import { ObjectTypeComponent } from "./common/formly/object.type";
import { UserService } from "./common/service/user/user.service";
import { CodeEditorComponent } from "./workspace/component/code-editor-dialog/code-editor.component";
import { AnnotationSuggestionComponent } from "./workspace/component/code-editor-dialog/annotation-suggestion.component";
import { CodeareaCustomTemplateComponent } from "./workspace/component/codearea-custom-template/codearea-custom-template.component";
import { MenuComponent } from "./workspace/component/menu/menu.component";
import { OperatorLabelComponent } from "./workspace/component/left-panel/operator-menu/operator-label/operator-label.component";
import { OperatorMenuComponent } from "./workspace/component/left-panel/operator-menu/operator-menu.component";
import { PropertyEditorComponent } from "./workspace/component/property-editor/property-editor.component";
import { TypeCastingDisplayComponent } from "./workspace/component/property-editor/typecasting-display/type-casting-display.component";
import { WorkflowEditorComponent } from "./workspace/component/workflow-editor/workflow-editor.component";
import { WorkspaceComponent } from "./workspace/component/workspace.component";
import { NzCardModule } from "ng-zorro-antd/card";
import { NzTagModule } from "ng-zorro-antd/tag";
import { NzAvatarModule } from "ng-zorro-antd/avatar";
import { BlobErrorHttpInterceptor } from "./common/service/blob-error-http-interceptor.service";
import { OperatorPropertyEditFrameComponent } from "./workspace/component/property-editor/operator-property-edit-frame/operator-property-edit-frame.component";
import { NzTabsModule } from "ng-zorro-antd/tabs";
import { NzPaginationModule } from "ng-zorro-antd/pagination";
import { JwtModule } from "@auth0/angular-jwt";
import { AuthService } from "./common/service/user/auth.service";
import { NzCommentModule } from "ng-zorro-antd/comment";
import { NzPopconfirmModule } from "ng-zorro-antd/popconfirm";
import { ContextMenuComponent } from "./workspace/component/workflow-editor/context-menu/context-menu/context-menu.component";
import { CoeditorUserIconComponent } from "./workspace/component/menu/coeditor-user-icon/coeditor-user-icon.component";
import { InputAutoCompleteComponent } from "./workspace/component/input-autocomplete/input-autocomplete.component";
import { CollabWrapperComponent } from "./common/formly/collab-wrapper/collab-wrapper/collab-wrapper.component";
import { NzSwitchModule } from "ng-zorro-antd/switch";
import { NzLayoutModule } from "ng-zorro-antd/layout";
import { AuthGuardService } from "./common/service/user/auth-guard.service";
import { MarkdownModule } from "ngx-markdown";
import { DragDropModule } from "@angular/cdk/drag-drop";
import { PortPropertyEditFrameComponent } from "./workspace/component/property-editor/port-property-edit-frame/port-property-edit-frame.component";
import { FormlyNgZorroAntdModule } from "@ngx-formly/ng-zorro-antd";
import { NzAlertModule } from "ng-zorro-antd/alert";
import { LeftPanelComponent } from "./workspace/component/left-panel/left-panel.component";
import { NzResizableModule } from "ng-zorro-antd/resizable";
import { NzMessageModule } from "ng-zorro-antd/message";
import { NzModalModule } from "ng-zorro-antd/modal";
import { OverlayModule } from "@angular/cdk/overlay";
import { en_US, provideNzI18n } from "ng-zorro-antd/i18n";
import { NzSpinModule } from "ng-zorro-antd/spin";
import { NgxFileDropModule } from "ngx-file-drop";
import { NzTreeModule } from "ng-zorro-antd/tree";
import { NzTreeViewModule } from "ng-zorro-antd/tree-view";
import { NzNoAnimationModule } from "ng-zorro-antd/core/no-animation";
import { TreeModule } from "@ali-hm/angular-tree-component";
import { FileSelectionComponent } from "./workspace/component/file-selection/file-selection.component";
import { GoogleAuthService } from "./common/service/user/google-auth.service";
import { SocialLoginModule, SocialAuthServiceConfig, GoogleSigninButtonModule } from "@abacritt/angularx-social-login";
import { GoogleLoginProvider } from "@abacritt/angularx-social-login";
import { lastValueFrom } from "rxjs";
import { NzEmptyModule } from "ng-zorro-antd/empty";
import { NzDividerModule } from "ng-zorro-antd/divider";
import { NzProgressModule } from "ng-zorro-antd/progress";
import { NzSliderModule } from "ng-zorro-antd/slider";

registerLocaleData(en);

@NgModule({
  declarations: [
    AppComponent,
    WorkspaceComponent,
    MenuComponent,
    OperatorMenuComponent,
    PropertyEditorComponent,
    WorkflowEditorComponent,
    OperatorLabelComponent,
    OperatorLabelComponent,
    ArrayTypeComponent,
    ObjectTypeComponent,
    MultiSchemaTypeComponent,
    NullTypeComponent,
    CodeareaCustomTemplateComponent,
    CodeEditorComponent,
    AnnotationSuggestionComponent,
    TypeCastingDisplayComponent,
    OperatorPropertyEditFrameComponent,
    OperatorPropertyEditFrameComponent,
    LeftPanelComponent,
    ContextMenuComponent,
    CoeditorUserIconComponent,
    InputAutoCompleteComponent,
    FileSelectionComponent,
    CollabWrapperComponent,
    PortPropertyEditFrameComponent,
  ],
  imports: [
    BrowserModule,
    AppRoutingModule,
    HttpClientModule,
    JwtModule.forRoot({
      config: {
        tokenGetter: AuthService.getAccessToken,
        skipWhenExpired: false,
        throwNoTokenError: false,
        disallowedRoutes: ["forum/api/users"],
      },
    }),
    BrowserAnimationsModule,
    RouterModule.forRoot([]),
    FormsModule,
    ReactiveFormsModule,
    FormlyModule.forRoot(TEXERA_FORMLY_CONFIG),
    FormlyNgZorroAntdModule,
    OverlayModule,
    NzDatePickerModule,
    NzDropDownModule,
    NzButtonModule,
    NzAutocompleteModule,
    NzIconModule,
    NzFormModule,
    NzListModule,
    NzInputModule,
    NzPopoverModule,
    NzCollapseModule,
    NzToolTipModule,
    NzTableModule,
    NzSelectModule,
    NzSpaceModule,
    NzBadgeModule,
    NzUploadModule,
    NgxJsonViewerModule,
    NzMessageModule,
    NzModalModule,
    NzCardModule,
    NzTagModule,
    NzPopconfirmModule,
    NzAvatarModule,
    NzTabsModule,
    NzPaginationModule,
    NzCommentModule,
    ColorPickerModule,
    NzSwitchModule,
    NzLayoutModule,
    NzSliderModule,
    MarkdownModule.forRoot(),
    DragDropModule,
    NzAlertModule,
    NzResizableModule,
    NzSpinModule,
    NgxFileDropModule,
    NzTreeModule,
    NzTreeViewModule,
    NzNoAnimationModule,
    TreeModule,
    SocialLoginModule,
    GoogleSigninButtonModule,
    NzEmptyModule,
    NzDividerModule,
    NzProgressModule,
  ],
  providers: [
    provideNzI18n(en_US),
    AuthGuardService,
    DatePipe,
    UserService,
    {
      provide: HTTP_INTERCEPTORS,
      useClass: BlobErrorHttpInterceptor,
      multi: true,
    },
    {
      provide: "SocialAuthServiceConfig",
      useFactory: (googleAuthService: GoogleAuthService, userService: UserService) => {
        return lastValueFrom(googleAuthService.getClientId()).then(clientId => ({
          providers: [
            {
              id: GoogleLoginProvider.PROVIDER_ID,
              provider: new GoogleLoginProvider(clientId, { oneTapEnabled: !userService.isLogin() }),
            },
          ],
        })) as Promise<SocialAuthServiceConfig>;
      },
      deps: [GoogleAuthService, UserService],
    },
  ],
  bootstrap: [AppComponent],
})
export class AppModule {}
