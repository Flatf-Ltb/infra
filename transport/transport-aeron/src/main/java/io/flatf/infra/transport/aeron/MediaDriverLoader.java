package io.flatf.infra.transport.aeron;

import io.aeron.driver.MediaDriver;

public class MediaDriverLoader {

    public static void main(String[] args) {

        MediaDriver.launchEmbedded();

    }
}
