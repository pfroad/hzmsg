package io.hzmsg.queue;

import io.hzmsg.persist.IMessageStore;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.HazelcastClientNotActiveException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import io.moquette.interception.messages.HazelcastMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by Ryan on 2017/6/12.
 */
public class HZQueueReceiver implements Runnable {
	private static final Logger LOG = LoggerFactory.getLogger(HZQueueReceiver.class);
	private boolean isStopped = false;

	private IQueue<HazelcastMessage> queue;
	private HZQueueListener hzQueueListener;
	private final Thread thread;
	private final String queueName;
	private HazelcastInstance hazelcastInstance;
	private final IMessageStore<HazelcastMessage> messageStore;
	private BlockingQueue<String> receivedMsgQueue;
//	private int hzReconnectPeriod = 3000;
//	private int hzConnectionAttemptTimes = 2;
//	private int hzAttemptedTimes = 0;


	public HZQueueReceiver(IMessageStore<HazelcastMessage> messageStore,
                           BlockingQueue<String> receivedMsgQueue,
                           HZQueueListener hzQueueListener,
                           HazelcastInstance hazelcastInstance,
                           String queueName) {
	    this.receivedMsgQueue = receivedMsgQueue;
		this.hzQueueListener = hzQueueListener;
		this.hazelcastInstance = hazelcastInstance;
		this.queueName = queueName;
		this.queue = hazelcastInstance.getQueue(queueName);
		this.messageStore = messageStore;
		this.thread = new Thread(this, HZQueueReceiver.class.getSimpleName());
	}

	public void run() {
		while (!isStopped) {
			try {
				HazelcastMessage message = queue.poll(1000L, TimeUnit.MILLISECONDS);

				if (message != null) {
				    String messageId = message.getMessageId();
				    while (true) {
                        try {
                            if (receivedMsgQueue.offer(messageId, 1000, TimeUnit.MILLISECONDS)) {
                                messageStore.put(messageId, message);
                                break;
                            }
                        } catch (InterruptedException e) {
                        }
                    }
//					queueConsumer.consume(message, hzQueueListener);
				}
			} catch (HazelcastClientNotActiveException he) {
				LOG.info("Hazelcast is not active, try to reconnect!");
				while (true) {
					try {
						hazelcastInstance.shutdown();	// close old hazelcastInstance client
						try {
							Thread.sleep(3000);
						} catch (InterruptedException e) {
						}
						hazelcastInstance = HazelcastClient.newHazelcastClient();	// create new hazelcastInstance client and assign to bean
						queue = hazelcastInstance.getQueue(queueName);
						break;
					} catch (Exception hze) {
						LOG.error("Failed to get hzQueue!", hze);
					}
				}
			} catch (Exception e) {
				LOG.error("Consumer handle message is failed!", e);
			}
		}
	}

	public void start() {
		this.thread.start();
		this.isStopped = false;
		LOG.info("Queue message receiver is started.");
	}

	public void shutdown() {
		this.shutdown(false);
	}

	public void shutdown(final boolean interrupt) {
		this.isStopped = true;
		LOG.info("shutdown thread " + HZQueueReceiver.class.getSimpleName() + " interrupt " + interrupt);

		if (interrupt) {
			this.thread.interrupt();
		}
	}

	public boolean isStopped() {
		return isStopped;
	}

	public void setStopped(boolean stopped) {
		isStopped = stopped;
	}

	public IQueue<HazelcastMessage> getQueue() {
		return queue;
	}

	public void setQueue(IQueue<HazelcastMessage> queue) {
		this.queue = queue;
	}

	public HZQueueListener getHzQueueListener() {
		return hzQueueListener;
	}

	public void setHzQueueListener(HZQueueListener hzQueueListener) {
		this.hzQueueListener = hzQueueListener;
	}
}
