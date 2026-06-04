package io.flatf.infra.actors.example;

import akka.actor.typed.ActorSystem;
import io.flatf.infra.actors.example.msg.SayHello;
import io.flatf.common.log4j2.Log4j2Configurator;
import io.flatf.common.log4j2.Log4j2Configurator.LogLevel;

import java.io.IOException;

/**
 * @author Akka official
 */
public class AkkaQuickstart {

    static {
        Log4j2Configurator.setLogFilename("actor-test");
        Log4j2Configurator.setLogLevel(LogLevel.INFO);
    }

    public static void main(String[] args) {

        // #actor-system
        final ActorSystem<SayHello> greeterMain = ActorSystem.create(GreeterMain.create(), "hello-akka");
        // #actor-system

        // #main-send-messages
        greeterMain.tell(new SayHello("Charles"));
        // #main-send-messages

        try {
            System.out.println(">>> Press ENTER to exit <<<");
            System.in.read();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            greeterMain.terminate();
        }

    }

}
