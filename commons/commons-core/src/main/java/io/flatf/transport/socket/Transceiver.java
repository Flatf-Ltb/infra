package io.flatf.transport.socket;

import io.flatf.transport.api.Receiver;
import io.flatf.transport.api.Sender;

public interface Transceiver<T> extends Receiver {

	Sender<T> getSender();

	boolean startSend();

}
