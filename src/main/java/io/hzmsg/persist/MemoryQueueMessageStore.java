package io.hzmsg.persist;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.moquette.interception.messages.HazelcastMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by Ryan on 2017/6/9.
 */
public class MemoryQueueMessageStore implements IMessageStore<HazelcastMessage> {
    private final static Logger LOG = LoggerFactory.getLogger(MemoryQueueMessageStore.class);
    private ConcurrentMap<String, byte[]> messageStore;

    public MemoryQueueMessageStore() {
    }

    public HazelcastMessage get(String messageId) {
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
        messageStore = new ConcurrentHashMap<>();
    }

    public void close() {
    }
}