package io.flatf.transport.http.ws;

import io.flatf.common.functional.Handler;
import org.asynchttpclient.ws.WebSocket;

public interface WebSocketHandler {

    @FunctionalInterface
    interface OpenHandler extends Handler<WebSocket> {
    }

    @FunctionalInterface
    interface CloseHandler {
        void handle(WebSocket webSocket, int code, String reason);
    }

    @FunctionalInterface
    interface PingFrameHandler extends Handler<byte[]> {
    }

    @FunctionalInterface
    interface PongFrameHandler extends Handler<byte[]> {
    }

    @FunctionalInterface
    interface BinaryFrameHandler {
        void handle(byte[] payload, boolean finalFragment, int rsv);
    }

    @FunctionalInterface
    interface TextFrameHandler {
        void handle(String payload, boolean finalFragment, int rsv);
    }

}
