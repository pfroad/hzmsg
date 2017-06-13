package io.hzmsg.queue;

import io.hzmsg.persist.IMessageStore;
import io.moquette.interception.messages.HazelcastMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.concurrent.BlockingQueue;

/**
 * Created by ryan on 6/12/17.
 */
public class RestoreQueueProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(RestoreQueueProcessor.class);
    private final IMessageStore<HazelcastMessage> messageStore;
    private final BlockingQueue<String> receivedMsgQueue;

    public RestoreQueueProcessor(IMessageStore<HazelcastMessage> messageStore, BlockingQueue<String> receivedMsgQueue) {
        this.messageStore = messageStore;
        this.receivedMsgQueue = receivedMsgQueue;
    }

    public void restore() {
        LOG.info("Restore received msg that saved message store.");
        messageStore.getAll()
                .stream()
                .sorted(Comparator.comparing(HazelcastMessage::getTimestamp))
                .forEach(message -> {
                        try {
                            receivedMsgQueue.add(message.getMessageId());
                        } catch (IllegalStateException e) {
                            LOG.error("Restore message error!", e);
                        }
                });
    }
}
