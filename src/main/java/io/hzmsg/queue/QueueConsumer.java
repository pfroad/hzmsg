package io.hzmsg.queue;

import io.moquette.interception.messages.HazelcastMessage;

/**
 * Created by ryan on 5/15/17.
 */
public interface QueueConsumer{
    void start();

    void shutdown();

    void consume(final HazelcastMessage hazelcastMessage);

    boolean retry(final HazelcastMessage hazelcastMessage);
}
