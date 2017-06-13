package io.hzmsg.queue;

import io.hzmsg.Message;

public class QueueMessage extends Message {
	private String queue;

	public String getQueue() {
		return queue;
	}

	public void setQueue(String queue) {
		this.queue = queue;
	}

}