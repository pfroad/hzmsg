package io.hzmsg.persist;

import java.util.List;
import java.util.Set;

/**
 * Created by Ryan on 2017/6/9.
 */
public interface IMessageStore<T> {
    T get(String messageId);

    void remove(String messageId);

    void put(String messageId, T t);

    List<T> getAll();

    void init();

    void close();
}
