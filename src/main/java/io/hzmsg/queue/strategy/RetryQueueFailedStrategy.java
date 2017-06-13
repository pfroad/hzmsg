package io.hzmsg.queue.strategy;

import io.hzmsg.queue.QueueConsumer;
import io.moquette.interception.messages.HazelcastMessage;

public class RetryQueueFailedStrategy implements QueueFailedStrategy {

	private QueueConsumer queueConsumer;

	public void handleFailed(final HazelcastMessage hazelcastMessage) throws InterruptedException {
		queueConsumer.retry(hazelcastMessage);
	}

	public QueueConsumer getQueueConsumer() {
		return queueConsumer;
	}

	public void setQueueConsumer(QueueConsumer queueConsumer) {
		this.queueConsumer = queueConsumer;
	}
}
