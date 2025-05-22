import { ComponentFixture, TestBed } from "@angular/core/testing";

import { ChatAssistantMultiAgentComponent } from "./chat-assistant-multi-agent.component";

describe("ChatAssistantComponent", () => {
  let component: ChatAssistantMultiAgentComponent;
  let fixture: ComponentFixture<ChatAssistantMultiAgentComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [ChatAssistantMultiAgentComponent],
    });
    fixture = TestBed.createComponent(ChatAssistantMultiAgentComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
