package io.hzmsg.queue;

import io.hzmsg.persist.IMessageStore;
import io.moquette.interception.messages.HazelcastMessage;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by Ryan on 2017/6/12.
 */
public class QueueConsumeProcessor implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(QueueConsumeProcessor.class);
    private IMessageStore<HazelcastMessage> messageStore;
    private BlockingQueue<String> receivedMsgQueue;
    private QueueConsumer queueConsumer;

    private Thread thread;

    private boolean isStopped = false;

    public QueueConsumeProcessor(IMessageStore<HazelcastMessage> messageStore,
                                 BlockingQueue<String> receivedMsgQueue,
                                 QueueConsumer queueConsumer) {
        this.messageStore = messageStore;
        this.receivedMsgQueue = receivedMsgQueue;
        this.queueConsumer = queueConsumer;
        this.thread = new Thread(this, this.getClass().getSimpleName());
    }

    @Override
    public void run() {
        while (!isStopped) {
            try {
                String messageId = receivedMsgQueue.poll(1000l, TimeUnit.MILLISECONDS);

                if (StringUtils.isNotEmpty(messageId)) {
                    HazelcastMessage message = messageStore.get(messageId);
                    if (message != null) {
                        queueConsumer.consume(message);
                    } else {
                        LOG.info("No message is found with this messageId = {}, discard it.", messageId);
                    }
                }
            } catch (InterruptedException e) {
            }
        }
    }

    public void start() {
        this.thread.start();
        this.isStopped = false;
        LOG.info("Queue message processor task is started.");
    }

    public void shutdown() {
        this.shutdown(false);
    }

    public void shutdown(final boolean interrupt) {
        this.isStopped = true;
        LOG.info("shutdown thread " + QueueConsumeProcessor.class.getSimpleName() + " interrupt " + interrupt);

        if (interrupt) {
            this.thread.interrupt();
        }
    }
}
