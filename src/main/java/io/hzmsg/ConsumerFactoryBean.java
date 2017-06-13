package io.hzmsg;

/**
 * Created by ryan on 5/15/17.
 */
public interface ConsumerFactoryBean {
    public Consumer getObject();

    public <T> Class<T> getObjectType();
}
