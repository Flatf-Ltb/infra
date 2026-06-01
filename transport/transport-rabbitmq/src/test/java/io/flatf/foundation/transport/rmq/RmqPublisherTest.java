package io.flatf.foundation.transport.rmq;

import io.flatf.foundation.transport.rmq.config.RmqConnectionConfig;
import io.flatf.foundation.transport.rmq.config.RmqProducerConfig;
import io.flatf.foundation.transport.rmq.declare.AmqpQueue;
import io.flatf.foundation.transport.rmq.declare.ExchangeRelationship;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class RmqPublisherTest {

	public static void main(String[] args) {

		RmqConnectionConfig connection = RmqConnectionConfig.with("10.0.64.201", 5672, "root", "root2018").build();

		RmqProducerConfig configurator = RmqProducerConfig
				.with(connection, ExchangeRelationship.fanout("TEST_DIR")
						.bindingQueues(List.of(AmqpQueue.named("TEST_D1")), Arrays.asList("K1", "K2")))
				.defaultRoutingKey("K1").build();

		try (RmqProducer publisher = new RmqProducer("TEST_PUB", configurator)) {
			publisher.publish("To_K1".getBytes());
			publisher.publish("K1", "To_K1_0".getBytes());
			publisher.publish("K2", "To_K2".getBytes());
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
