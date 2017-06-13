package io.hzmsg.queue;

import io.hzmsg.DataClientException;
import io.hzmsg.queue.strategy.QueueFailedStrategy;
import com.hazelcast.core.HazelcastInstance;

/**
 * Created by ryan on 5/15/17.
 */
public class QueueConsumerBean {
    private QueueConsumer queueConsumer;
    private HZQueueListener hzQueueListener;
    private HazelcastInstance hz;

    private String queueName;
    private QueueFailedStrategy failedStrategy;
    private int poolSize = 1;
    private int receivedMsgQueueCapacity = 100;
    private String persistFile;
    private int autoCommitInterval;

    public void start() {
        if (hz == null) {
            throw new DataClientException("Hazelcast instance cannot be null!");
        }

        if (hzQueueListener == null) {
            throw new DataClientException("Data listener cannot be null!");
        }

        if (queueName == null) {
            throw new DataClientException("Consumer queueName cannot be null!");
        }

        queueConsumer = new QueueConsumerImpl(hz, queueName, poolSize,
                receivedMsgQueueCapacity, hzQueueListener,
                failedStrategy, this.persistFile, this.autoCommitInterval);
        queueConsumer.start();
    }

    public void shutdown() {
        queueConsumer.shutdown();
    }

    public HZQueueListener getHzQueueListener() {
        return hzQueueListener;
    }

    public void setHzQueueListener(HZQueueListener hzQueueListener) {
        this.hzQueueListener = hzQueueListener;
    }

    public HazelcastInstance getHz() {
        return hz;
    }

    public void setHz(HazelcastInstance hz) {
        this.hz = hz;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public QueueFailedStrategy getFailedStrategy() {
        return failedStrategy;
    }

    public void setFailedStrategy(QueueFailedStrategy failedStrategy) {
        this.failedStrategy = failedStrategy;
    }

    public int getPoolSize() {
        return poolSize;
    }

    public void setPoolSize(int poolSize) {
        this.poolSize = poolSize;
    }

    public int getReceivedMsgQueueCapacity() {
        return receivedMsgQueueCapacity;
    }

    public void setReceivedMsgQueueCapacity(int receivedMsgQueueCapacity) {
        this.receivedMsgQueueCapacity = receivedMsgQueueCapacity;
    }

    public String getPersistFile() {
        return persistFile;
    }

    public void setPersistFile(String persistFile) {
        this.persistFile = persistFile;
    }

    public int getAutoCommitInterval() {
        return autoCommitInterval;
    }

    public void setAutoCommitInterval(int autoCommitInterval) {
        this.autoCommitInterval = autoCommitInterval;
    }
}
