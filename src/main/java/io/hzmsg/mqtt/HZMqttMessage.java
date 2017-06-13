package io.hzmsg.mqtt;

/**
 * Created by ryan on 5/21/17.
 */
public class HZMqttMessage {

    private final String topicName;
    private final boolean isRetain;
    private final int qos;
    private final byte[] payload;

    private String clientId = "hzClient";
    private String username;

    public HZMqttMessage(String topicName, byte[] payload, int qos, boolean isRetain) {
        this.topicName = topicName;
        this.isRetain = isRetain;
        this.qos = qos;
        this.payload = payload;
    }

    public String getTopicName() {
        return topicName;
    }

    public boolean isRetain() {
        return isRetain;
    }

    public int getQos() {
        return qos;
    }

    public byte[] getPayload() {
        return payload;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    //    HazelcastInstance client = HazelcastClient.newHazelcastClient();
//    ITopic<HazelcastMsg> topic = client.getTopic("moquette");
//
//
//    MqttMessage message = new MqttMessage();
//        message.setQos(1);
//        message.setRetained(true);
//        message.setPayload("This is test".getBytes());
//
//    int variableHeaderBufferSize = 2 + "topic/test".getBytes(Charset.forName("UTF-8")).length +
//            (message.getQos() > 0 ? 2 : 0);
//
//    MqttPublish mqttPublish = new MqttPublish("topic/test", message);
//    byte[] pl = mqttPublish.getPayload();
//    byte[] header = mqttPublish.getHeader();
//    MqttPublishVariableHeader variableHeader = new MqttPublishVariableHeader("topic/test", 0);
//    MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_LEAST_ONCE, false, 0);
//    MqttPublishVariableHeader varHeader = new MqttPublishVariableHeader("topic/test", 0);
//    ByteBuf payload = Unpooled.wrappedBuffer(pl);
//    MqttPublishMessage publishMessage = new MqttPublishMessage(fixedHeader, varHeader, payload);
//
//    HazelcastMsg hazelcastMsg = new HazelcastMsg(new InterceptPublishMessage(publishMessage, null, "", MD5Util.toMD5String("" + System.currentTimeMillis())));

//    topic.publish(hazelcastMsg);

}
