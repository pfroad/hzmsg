package io.hzmsg.annotations;

import java.lang.annotation.*;

/**
 * Created by ryan on 5/16/17.
 */
@Inherited
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface HzListener {
    String value();

    String hzKey();

    int retry() default 0;
}
