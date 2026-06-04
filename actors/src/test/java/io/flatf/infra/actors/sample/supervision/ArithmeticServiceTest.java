package io.flatf.infra.actors.sample.supervision;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.Status;
import akka.testkit.javadsl.TestKit;
import io.flatf.infra.actors.sample.supervision.Expression.Add;
import io.flatf.infra.actors.sample.supervision.Expression.Const;
import io.flatf.infra.actors.sample.supervision.Expression.Divide;
import io.flatf.infra.actors.sample.supervision.Expression.Multiply;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.stream.IntStream;

public class ArithmeticServiceTest {

    static ActorSystem system;

    @BeforeClass
    public static void setup() {
        system = ActorSystem.create("BuncherTest");
    }

    @AfterClass
    public static void tearDown() {
        TestKit.shutdownActorSystem(system);
        system = null;
    }

    @Test
    public void TheArithmeticServiceShouldCalculateConstantExpressionsProperly() {
        new TestKit(system) {
            {
                final ActorRef service = system.actorOf(Props.create(ArithmeticService.class));
                final ActorRef probe = getRef();
                IntStream.range(-2, 3).forEach(x -> {
                    service.tell(new Const(x), probe);
                    expectMsgEquals(x);
                });
            }
        };
    }

    @Test
    public void TheArithmeticServiceShouldCalculateAdditionProperly() {
        new TestKit(system) {
            {
                final ActorRef service = system.actorOf(Props.create(ArithmeticService.class));
                final ActorRef probe = getRef();
                IntStream.range(-2, 3).forEach(x -> IntStream.range(-2, 3).forEach(y -> {
                    service.tell(new Add(new Const(x), new Const(y)), probe);
                    expectMsgEquals(x + y);
                }));
            }
        };
    }

    @Test
    public void TheArithmeticServiceShouldCalculateMultiplicationAndDivisionProperly() {
        new TestKit(system) {
            {
                final ActorRef service = system.actorOf(Props.create(ArithmeticService.class));
                final ActorRef probe = getRef();
                IntStream.range(-2, 3).forEach(x -> IntStream.range(-2, 3).forEach(y -> {
                    service.tell(new Multiply(new Const(x), new Const(y)), probe);
                    expectMsgEquals(x * y);
                }));

                // Skip zero in the second parameter
                IntStream.range(-2, 3).forEach(x -> IntStream.of(-2, -1, 1, 2).forEach(y -> {
                    service.tell(new Divide(new Const(x), new Const(y)), probe);
                    expectMsgEquals(x / y);
                }));
            }
        };
    }

    @Test
    public void TheArithmeticServiceShouldSurviveIllegalExpressions() {
        new TestKit(system) {
            {
                final ActorRef service = system.actorOf(Props.create(ArithmeticService.class));
                final ActorRef probe = getRef();

                service.tell(new Divide(new Const(1), new Const(0)), probe);
                expectMsgClass(Status.Failure.class);

                service.tell(new Add(null, new Const(0)), probe);
                expectMsgClass(Status.Failure.class);

                service.tell(new Add(new Const(1), new Const(0)), probe);
                expectMsgEquals(1);
            }
        };
    }
}

