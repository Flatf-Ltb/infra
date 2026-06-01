package io.flatf.foundation.actors.example;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import io.flatf.foundation.actors.example.msg.Greet;
import io.flatf.foundation.actors.example.msg.Greeted;
import org.junit.jupiter.api.Test;

//#definition
public class AkkaQuickstartTest {

    public static final TestKitJunitResource TestKit = new TestKitJunitResource();
    // #definition

    // #test
    @Test
    public void testGreeterActorSendingOfGreeting() {
        TestProbe<Greeted> testProbe = TestKit.createTestProbe();
        ActorRef<Greet> underTest = TestKit.spawn(Greeter.create(), "greeter");
        underTest.tell(new Greet("Charles", testProbe.getRef()));
        testProbe.expectMessage(new Greeted("Charles", underTest));
    }
    // #test

}
