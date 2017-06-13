package io.hzmsg.queue.strategy;

import io.moquette.interception.messages.HazelcastMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultQueueFailedStrategy implements QueueFailedStrategy {
	private static final Logger LOG = LoggerFactory.getLogger(DefaultQueueFailedStrategy.class);

	public void handleFailed(final HazelcastMessage hazelcastMessage) {
		LOG.info("Failed to receive data. data={}", hazelcastMessage.toString());
	}

}
