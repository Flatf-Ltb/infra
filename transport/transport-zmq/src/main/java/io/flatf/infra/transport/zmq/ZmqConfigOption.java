package io.flatf.infra.transport.zmq;

import io.flatf.common.config.ConfigOption;

public enum ZmqConfigOption implements ConfigOption {

	PROTOCOL("zmq.protocol", "zeromq.protocol"),

	ADDR("zmq.addr", "zeromq.addr"),

	PORT("zmq.port", "zeromq.port"),

	IO_THREADS("zmq.ioThreads", "zeromq.ioThreads");

	private final String configName;

	private final String otherName;

	ZmqConfigOption(String configName, String otherName) {
		this.configName = configName;
		this.otherName = otherName;
	}

	@Override
	public String getConfigName() {
		return configName;
	}

	@Override
	public String getOtherName() {
		return otherName;
	}

}