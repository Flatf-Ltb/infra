package io.mercury.transport.socket;

import io.mercury.common.lang.Validator;
import io.mercury.transport.socket.configurator.SocketCfg;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class LocalSocketManager {

    private static final Map<String, SocketTransceiver> ServerSocketMap = new ConcurrentHashMap<>(8);

    /**
     * @param host     String
     * @param port     int
     * @param callback Consumer<byte[]>
     * @return SocketTransceiver
     */
    public static synchronized SocketTransceiver getSocketTransceiver(
            String host, int port, Consumer<byte[]> callback) {
        String socketName = socketName(host, port);
        if (ServerSocketMap.containsKey(socketName))
            return ServerSocketMap.get(socketName);
        else {
            Validator.atWithinRange(port, 7000, 8000, "port");
            SocketTransceiver transceiver = new SocketTransceiver(SocketCfg.builder().port(port).build(),
                    callback);
            ServerSocketMap.put(socketName, transceiver);
            return transceiver;
        }
    }

    /**
     * Return SocketTransceiver obj or null.
     *
     * @param name String
     * @param port int
     * @return SocketTransceiver
     */
    public static SocketTransceiver acquireSocketTransceiver(String name, int port) {
        return ServerSocketMap.getOrDefault(name, null);
    }

    private static String socketName(String name, int port) {
        return name + ":" + port;
    }

}
