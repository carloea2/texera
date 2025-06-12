import { Component, OnInit } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { ChatAssistantWebsocketService } from "../../../service/chat-assistant-websocket/chat-assistant-websocket.service";

@UntilDestroy()
@Component({
  selector: "texera-chat-assistant-non-streamed",
  templateUrl: "./chat-assistant-multi-agent.component.html",
  styleUrls: ["./chat-assistant-multi-agent.component.css"],
})
export class ChatAssistantMultiAgentComponent implements OnInit {
  messages: { sender: string; content: string }[] = [];
  newMessage: string = "";
  isWaitingForResponse = false;
  // Keeps track of the last AI message so we can append text deltas to it.
  private aiMessageIndex?: number;

  constructor(private chatAssistantWebsocketService: ChatAssistantWebsocketService) {}

  ngOnInit(): void {
    // Start WebSocket connection and subscribe to streaming response events.
    this.connectToPythonServer();
    this.subscribeToStreamResponses();
  }

  sendMessage() {
    if (!this.newMessage.trim()) {
      return;
    }

    const userMsg = this.newMessage.trim();
    this.messages.push({ sender: "User", content: userMsg });
    this.newMessage = "";
    this.isWaitingForResponse = true;

    // Send the user message to the Python server.
    this.chatAssistantWebsocketService.send("ChatUserMessageRequest", { message: userMsg });

    // Add an empty AI message entry; this will be updated as responses stream in.
    this.aiMessageIndex = this.messages.push({ sender: "AI", content: "" }) - 1;
  }

  private connectToPythonServer(): void {
    this.chatAssistantWebsocketService.closeWebsocket();
    this.chatAssistantWebsocketService.openWebsocket();
  }

  private subscribeToStreamResponses(): void {
    // Subscribe to each delta of the AI response.
    this.chatAssistantWebsocketService
      .subscribeToEvent("ChatStreamResponseEvent")
      .pipe(untilDestroyed(this))
      .subscribe({
        next: (event: { type: "ChatStreamResponseEvent"; delta: string }) => {
          if (this.aiMessageIndex !== undefined) {
            this.messages[this.aiMessageIndex].content += event.delta;
          }
        },
        error: (err: unknown) => {
          console.error("Error in chat stream:", err);
          this.isWaitingForResponse = false;
        },
      });

    // Subscribe to the stream completion event.
    this.chatAssistantWebsocketService
      .subscribeToEvent("ChatStreamResponseComplete")
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => {
          // Mark the stream as complete.
          this.isWaitingForResponse = false;
          this.aiMessageIndex = undefined;
          console.log("Chat stream completed.");
        },
        error: (err: unknown) => {
          console.error("Error in stream completion event:", err);
        },
      });
  }
}
