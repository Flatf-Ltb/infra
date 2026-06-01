package io.flatf.foundation.transport.zmq;

/**
 * 当前支持的协议类型
 *
 * @author yellow013
 */
public enum ZmqProtocol {

    TCP("tcp://"),

    IPC("ipc://"),

    INPROC("inproc://");

    private final String prefix;

    ZmqProtocol(String prefix) {
        this.prefix = prefix;
    }

    public String prefix() {
        return prefix;
    }

    public ZmqAddr addr(String addr) {
        return new ZmqAddr(this, addr);
    }

    @Override
    public String toString() {
        return name();
    }

    /**
     * 根据名称检索协议对象
     *
     * @param name String
     * @return ZmqProtocol
     */
    public static ZmqProtocol of(String name) {
        for (ZmqProtocol protocol : ZmqProtocol.values()) {
            if (protocol.prefix.startsWith(name.toLowerCase()))
                return protocol;
        }
        throw new IllegalArgumentException("Unsupported protocol -> " + name);
    }

}