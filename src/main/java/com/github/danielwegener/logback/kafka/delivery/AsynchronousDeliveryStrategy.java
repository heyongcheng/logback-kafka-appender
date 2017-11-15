package com.github.danielwegener.logback.kafka.delivery;

import org.apache.kafka.clients.producer.BufferExhaustedException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * @since 0.0.1
 */
public class AsynchronousDeliveryStrategy implements DeliveryStrategy {

    @Override
    public <K, V, E> boolean send(Producer<K, V> producer, ProducerRecord<K, V> record, final E event,
                                  final FailedDeliveryCallback<E> failedDeliveryCallback) {
        try {
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                        failedDeliveryCallback.onFailedDelivery(event, exception);
                    }else {
                        if (DeliveryStrategySupport.INSTANCE.isOff()){
                            DeliveryStrategySupport.INSTANCE.setOff(false);
                        }
                        if (DeliveryStrategySupport.INSTANCE.getFailCount().get() > 0) {
                            DeliveryStrategySupport.INSTANCE.getFailCount().set(0);
                        }
                    }
                }
            });
            return true;
        } catch (BufferExhaustedException e) {
            failedDeliveryCallback.onFailedDelivery(event, e);
            return false;
        }
    }

}
