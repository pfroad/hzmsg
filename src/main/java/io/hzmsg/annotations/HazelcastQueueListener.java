package io.hzmsg.annotations;

import java.lang.annotation.*;

@Inherited
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface HazelcastQueueListener {
    String queue();

    int retry() default 0;
}