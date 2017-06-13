package io.hzmsg.persist;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.moquette.interception.messages.HazelcastMessage;
import org.mapdb.DB;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by Ryan on 2017/6/9.
 */
public class MapDBQueueMessageStore implements IMessageStore<HazelcastMessage> {
    private final static Logger LOG = LoggerFactory.getLogger(MapDBQueueMessageStore.class);
    private final DB db;
    private ConcurrentMap<String, byte[]> messageStore;

    private ScheduledExecutorService scheduledExecutorService;
    private final int autoCommitInterval;

    public MapDBQueueMessageStore(DB db, ScheduledExecutorService scheduledExecutorService, int autoCommitInterval) {
        this.db = db;
        this.scheduledExecutorService = scheduledExecutorService;
        this.autoCommitInterval = autoCommitInterval;
    }

    public HazelcastMessage get(String messageId) {
        if (!messageStore.containsKey(messageId)) return null;
        return JSON.parseObject(messageStore.get(messageId), HazelcastMessage.class);
    }

    public void remove(String messageId) {
        LOG.debug("Removing message......");
        messageStore.remove(messageId);
    }

    public void put(String messageId, HazelcastMessage hazelcastMessage) {
        LOG.debug("Storing message......");
        messageStore.put(messageId, JSONObject.toJSONBytes(hazelcastMessage));
    }

    @Override
    public List<HazelcastMessage> getAll() {
        Set<String> keys = messageStore.keySet();
        final List<HazelcastMessage> messages = new ArrayList<>(keys.size());
        keys.forEach(key -> messages.add(this.get(key)));
        return messages;
    }

    public void init() {
        LOG.info("Initializing MapDB message store......");
        messageStore = this.db.hashMap("queueMessage").keySerializer(Serializer.STRING).valueSerializer(Serializer.BYTE_ARRAY).createOrOpen();
        LOG.info("Scheduling MapDB commit task...");
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            public void run() {
                LOG.debug("Commit to MapDB......");
                db.commit();
            }
        }, autoCommitInterval, autoCommitInterval, TimeUnit.SECONDS);
    }

    public void close() {
        if (db.isClosed()) {
            LOG.warn("MapDB has been closed. Nothing will be done!");
            return;
        }

        LOG.info("Performing last commit to DB......");
        db.commit();
        LOG.info("Closing MapDB......");
        db.close();

        this.scheduledExecutorService.shutdown();
        try {
            this.scheduledExecutorService.awaitTermination(10L, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
        }

        if (!this.scheduledExecutorService.isTerminated()) {
            LOG.warn("Forcing shutdown MapDB commit tasks......");
            this.scheduledExecutorService.shutdown();
        }
        LOG.info("Message store has been closed successfully.");
    }
}
