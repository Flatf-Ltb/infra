package io.flatf.transport.rmq;

import io.flatf.transport.rmq.config.RmqConnection;
import io.flatf.transport.rmq.config.RmqPublisherConfig;
import io.flatf.transport.rmq.declare.AmqpQueue;
import io.flatf.transport.rmq.declare.ExchangeRelationship;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class RmqPublisherTest {

	public static void main(String[] args) {

		RmqConnection connection = RmqConnection.with("10.0.64.201", 5672, "root", "root2018").build();

		RmqPublisherConfig configurator = RmqPublisherConfig
				.configuration(connection, ExchangeRelationship.fanout("TEST_DIR")
						.bindingQueues(List.of(AmqpQueue.named("TEST_D1")), Arrays.asList("K1", "K2")))
				.setDefaultRoutingKey("K1").build();

		try (RmqPublisher publisher = new RmqPublisher("TEST_PUB", configurator)) {
			publisher.publish("To_K1".getBytes());
			publisher.publish("K1", "To_K1_0".getBytes());
			publisher.publish("K2", "To_K2".getBytes());
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
