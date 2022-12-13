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

public class Producer implements ExceptionListener {

    public static final String QUEUE_NAME = "test_queue";
    public static final String BROKER_URL = "tcp://localhost:61616";

    public static final Logger log = LoggerFactory.getLogger(Producer.class);

    public static void main(String[] args) throws JMSException {
        Producer producer = new Producer();
        producer.send("test!123123");
    }

    private void send(String msg) throws JMSException {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(BROKER_URL);

        Connection connection = connectionFactory.createConnection();
        connection.setExceptionListener(this);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // Create the destination (Topic or Queue)
        Destination destination = session.createQueue(QUEUE_NAME);

        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);

        TextMessage message = session.createTextMessage(msg);
        producer.send(message);
        log.info("[Producer] Message \"{}\" was sent to: {}", msg, QUEUE_NAME);

        producer.close();
        session.close();
        connection.close();
    }

    @Override
    public void onException(JMSException ex) {
        log.error("Error with JMS occurred", ex);
    }
}

