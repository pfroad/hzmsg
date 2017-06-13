package io.hzmsg.mqtt;

import org.apache.commons.lang3.StringUtils;

/**
 * Created by ryan on 5/23/17.
 */
public class MQTTUtils {
    public static String deviceId(String group, String separator, String id) {
        StringBuilder sb = new StringBuilder();
        sb.append(group);
        sb.append(separator);
        sb.append(id);
        return sb.toString();
    }

    public static String p2pTopic(String topic, String p2p, String group, String separator, String id) {
        StringBuilder sb = new StringBuilder();
        sb.append(topic);
        sb.append("/");
        sb.append(p2p);
        sb.append("/");
        sb.append(group);
        sb.append(separator);
        sb.append(id);
        return sb.toString();
    }

    public static String getDeviceIdFromTopic(String topicName) {
        if (StringUtils.isEmpty(topicName))
            return null;

        return topicName.substring(topicName.lastIndexOf("/") + 1);
    }
}
