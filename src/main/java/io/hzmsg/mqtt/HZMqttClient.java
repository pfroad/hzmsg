package io.hzmsg.mqtt;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ITopic;
import com.hazelcast.util.MD5Util;
import io.moquette.interception.HazelcastMsg;
import io.moquette.interception.messages.InterceptPublishMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Created by ryan on 5/21/17.
 */
public class HZMqttClient {
    private static final Logger LOG = LoggerFactory.getLogger(HZMqttClient.class);

    private HazelcastInstance hazelcastInstance;
    private String hzTopic = "moquette";
    private final ITopic<HazelcastMsg> topic;
    private int executorSize = 1;
    private ExecutorService executorService;

    public HZMqttClient(HazelcastInstance hazelcastInstance, int executorSize) {
        this.hazelcastInstance = hazelcastInstance;
        this.topic = this.hazelcastInstance.getTopic(hzTopic);
        this.executorSize = executorSize;
        this.executorService = Executors.newFixedThreadPool(executorSize, new ThreadFactory() {
            public Thread newThread(Runnable r) {
                return new Thread(r, "HazelcastMqttMessagePublishThread");
            }
        });
    }

    public HZMqttClient(HazelcastInstance hazelcastInstance) {
        this(hazelcastInstance, 1);
    }

    public void send(HZMqttMessage mqttMessage) {
        LOG.info("publish mqtt message to broker. topicName = {}, qos = {}, isRetain = {}", mqttMessage.getTopicName(), mqttMessage.getQos(), mqttMessage.isRetain());
        HazelcastMsg hazelcastMsg = createHazelcastMsg(mqttMessage);

        if (hazelcastMsg == null) {
            return;
        }

        doSent(hazelcastMsg);
        LOG.info("end to publish.");
    }

    private void doSent(final HazelcastMsg hazelcastMsg) {
        this.executorService.execute(new Runnable() {
            public void run() {
                topic.publish(hazelcastMsg);
            }
        });
    }

    public HazelcastMsg createHazelcastMsg(HZMqttMessage mqttMessage) {
        String mqttTopicName = mqttMessage.getTopicName();

        if (StringUtils.isEmpty(mqttTopicName)) {
            LOG.info("Message topic name cannot be null!");
            return null;
        }

        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.valueOf(mqttMessage.getQos()), mqttMessage.isRetain(), 0);
        MqttPublishVariableHeader varHeader = new MqttPublishVariableHeader(mqttMessage.getTopicName(), 0);
        ByteBuf payload = Unpooled.wrappedBuffer(mqttMessage.getPayload());
        MqttPublishMessage publishMessage = new MqttPublishMessage(fixedHeader, varHeader, payload);

        return new HazelcastMsg(new InterceptPublishMessage(publishMessage, mqttMessage.getClientId(),
                mqttMessage.getUsername(), MD5Util.toMD5String(mqttMessage.getClientId() + System.currentTimeMillis())));
    }

    public int getExecutorSize() {
        return executorSize;
    }

    public void setExecutorSize(int executorSize) {
        this.executorSize = executorSize;
    }

    public void close() {
        LOG.info("shutdown hzMqttClient......");
        executorService.shutdown();
    }
}
