package com.kaoruk;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

public class MyProducer {
    private final AmazonSQS sqs;
    private final String queueUrl;
    private final ExecutorService pool;
    private final String messageGroupId = "kaoru-test-one";
    private final AtomicBoolean running = new AtomicBoolean(true);

    public MyProducer(AmazonSQS sqs, String queueUrl, ExecutorService pool) {
        this.sqs = sqs;
        this.queueUrl = queueUrl;
        this.pool = pool;
    }

    public void start() {
        for (int num = 0; num < 1; num++) {
            pool.submit(new Sender("Sender " + (num)));
        }
    }

    public void stop() {
        running.set(false);
    }

    public class Sender implements Runnable {
        private final String id;
        private int count = 0;

        public Sender(String id) {
            this.id = id;
        }

        @Override
        public void run() {
            while(running.get() && count < 100) {
                SendMessageRequest req = new SendMessageRequest(
                        queueUrl,
                        id + ": This is my " + count + " message." + UUID.randomUUID()
                );
                req.setMessageGroupId(UUID.randomUUID().toString());
                req.setMessageDeduplicationId(id.replaceAll(" ", "") + count);

                try {
                    sqs.sendMessage(req);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                count++;
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
