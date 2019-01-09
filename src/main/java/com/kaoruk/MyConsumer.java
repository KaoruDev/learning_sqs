package com.kaoruk;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;

import java.util.List;

public class MyConsumer {
    private int count = 0;
    private final String queueUrl;
    private final AmazonSQS sqs;

    public MyConsumer(String queueUrl, AmazonSQS sqs) {
        this.queueUrl = queueUrl;
        this.sqs = sqs;
    }

    public void run() {
        ReceiveMessageRequest request = new ReceiveMessageRequest(queueUrl)
                .withMaxNumberOfMessages(10);
        for (; count < 100; count++) {
            List<Message> messages = sqs.receiveMessage(request).getMessages();
            System.out.println("Consumer received " + messages.size());
            for (Message msg : messages) {
                System.out.println("Message data: " + msg.getBody());

                if (!msg.getBody().contains("5")) {
                    sqs.deleteMessage(queueUrl, msg.getReceiptHandle());
                }

                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
