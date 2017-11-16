package com.github.danielwegener.logback.kafka.delivery;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author heyc
 */
public class DeliveryStrategySupport {

    public static DeliveryStrategySupport INSTANCE = new DeliveryStrategySupport();

    private Semaphore semaphore = new Semaphore(0);

    private KafkaDaemonTask kafkaDaemonTask = new KafkaDaemonTask();
    /**
     * 连续失败次数
     */
    private AtomicInteger failCount = new AtomicInteger(0);

    /**
     *  连续失败${failOffCount}后关闭
     */
    private int failOffCount = 3;

    public AtomicInteger getFailCount() {
        return failCount;
    }

    public void setFailCount(AtomicInteger failCount) {
        this.failCount = failCount;
    }

    public int getFailOffCount() {
        return failOffCount;
    }

    public void setFailOffCount(int failOffCount) {
        this.failOffCount = failOffCount;
    }

    /**
     * execute
     * @param producer
     * @param topic
     */
    public void execute(Producer<byte[], byte[]> producer,String topic) {
        execute(producer,topic,0);
    }

    /**
     * execute
     * @param producer
     * @param topic
     * @param requestTimeout
     */
    public void execute(Producer<byte[], byte[]> producer,String topic,long requestTimeout){
        kafkaDaemonTask.setProducer(producer);
        kafkaDaemonTask.setTopic(topic);
        if (requestTimeout > 0) {
            kafkaDaemonTask.setRequestTimeout(requestTimeout);
        }
        kafkaDaemonTask.execute();
    }

    /**
     * reconnect
     */
    public synchronized void reconnect(){
        semaphore.release();
    }

    class KafkaDaemonTask implements Runnable {

        private ExecutorService threadPool = Executors.newSingleThreadExecutor();

        Producer<byte[], byte[]> producer;

        String topic;

        long requestTimeout = 500;

        final byte[] value = "[RECONNECTION]".getBytes();

        /**
         * execute
         */
        public void execute() {
            threadPool.submit(this);
        }

        /**
         * When an object implementing interface <code>Runnable</code> is used
         * to create a thread, starting the thread causes the object's
         * <code>run</code> method to be called in that separately executing
         * thread.
         * <p>
         * The general contract of the method <code>run</code> is that it may
         * take any action whatsoever.
         *
         * @see Thread#run()
         */
        @Override
        public void run() {
            while (true){
                try {
                    semaphore.acquire();
                    while (DeliveryStrategySupport.INSTANCE.getFailCount().get() >= DeliveryStrategySupport.INSTANCE.getFailOffCount()) {
                        try {
                            producer.send(new ProducerRecord<byte[], byte[]>(topic, null, System.currentTimeMillis(), null, value), new Callback() {
                                @Override
                                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                                    if (e == null){
                                        DeliveryStrategySupport.INSTANCE.getFailCount().set(0);
                                    }
                                }
                            });
                        }catch (Exception e){
                            try {
                                Thread.sleep(3000);
                            } catch (InterruptedException e1) {
                            }
                        }
                    }
                }catch (Exception e){
                }
            }
        }

        public Producer<byte[], byte[]> getProducer() {
            return producer;
        }

        public void setProducer(Producer<byte[], byte[]> producer) {
            this.producer = producer;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public long getRequestTimeout() {
            return requestTimeout;
        }

        public void setRequestTimeout(long requestTimeout) {
            this.requestTimeout = requestTimeout;
        }
    }

}
