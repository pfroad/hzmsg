package io.hzmsg.queue;

import io.moquette.interception.messages.HazelcastMessage;

/**
 * Created by Administrator on 2017/6/9.
 */
public interface HZQueueListener {
    void handle(final HazelcastMessage hazelcastMessage);
}
