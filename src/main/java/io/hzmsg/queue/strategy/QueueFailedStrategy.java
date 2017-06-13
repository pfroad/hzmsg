package io.hzmsg.queue.strategy;

import io.moquette.interception.messages.HazelcastMessage;

public interface QueueFailedStrategy {
	void handleFailed(final HazelcastMessage hazelcastMessage) throws InterruptedException;
}
