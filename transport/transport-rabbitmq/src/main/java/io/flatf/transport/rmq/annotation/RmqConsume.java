package io.flatf.transport.rmq.annotation;

public @interface RmqConsume {

    String queueName();

    boolean autoAck() default true;

    String consumerTag() default "";

    boolean exclusive() default false;

}
