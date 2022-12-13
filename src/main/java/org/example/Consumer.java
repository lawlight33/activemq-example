package org.example;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

public class Consumer implements ExceptionListener {

    public static final String QUEUE_NAME = "test_queue";
    public static final String BROKER_URL = "tcp://localhost:61616";

    public static final Logger log = LoggerFactory.getLogger(Consumer.class);

    public static void main(String[] args) throws JMSException {
        Consumer consumer = new Consumer();
        consumer.receive();
    }

    public void receive() throws JMSException {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(BROKER_URL);

        Connection connection = connectionFactory.createConnection();
        connection.setExceptionListener(this);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // Create the destination (Topic or Queue)
        Destination destination = session.createQueue(QUEUE_NAME);

        MessageConsumer consumer = session.createConsumer(destination);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                consumer.close();
            } catch (JMSException e) { }
            try {
                session.close();
            } catch (JMSException e) { }
            try {
                connection.close();
            } catch (JMSException e) { }
        }));

        while (true) {
            log.info("[Consumer] Start receiving messages from: {}", QUEUE_NAME);
            Message received = consumer.receive();
            if (received instanceof TextMessage) {
                TextMessage textMessage = (TextMessage) received;
                String text = textMessage.getText();
                log.info("[Consumer] Message \"{}\" was received", text);
            } else {
                log.info("[Consumer] Message was received: {}", received);
            }
        }
    }

    @Override
    public void onException(JMSException ex) {
        log.error("Error with JMS occurred", ex);
    }
}

