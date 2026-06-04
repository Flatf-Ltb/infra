package io.flatf.infra.transport.aeron;

public final class Endpoint {

    public static Builder ipc() {
        return new Builder();
    }

    public static Builder udp() {
        return new Builder();
    }

    public static class Builder {


        private Builder() {

        }

    }

}
