package io.flatf.transport.zmq.annotation;


import io.flatf.transport.zmq.ZmqProtocol;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Documented
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface ZmqSubscribe {

    ZmqProtocol protocol();

    String addr();

    int ioThreads() default 1;

    String[] topic() default {""};

}
