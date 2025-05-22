import { TestBed } from "@angular/core/testing";

import { ChatAssistantWebsocketService } from "./chat-assistant-websocket.service";

describe("ChatAssistantWebsocketService", () => {
  let service: ChatAssistantWebsocketService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(ChatAssistantWebsocketService);
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });
});
