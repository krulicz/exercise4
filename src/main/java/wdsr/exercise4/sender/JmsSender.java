package wdsr.exercise4.sender;

import java.math.BigDecimal;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wdsr.exercise4.Order;

public class JmsSender {
	private static final Logger log = LoggerFactory.getLogger(JmsSender.class);

	private final String queueName;
	private final String topicName;
	private Connection connection;
	private Session session;
	Destination destination;
	MessageProducer producer;
	ConnectionFactory connectionFactory;

	public JmsSender(final String queueName, final String topicName) {
		this.queueName = queueName;
		this.topicName = topicName;
	}

	/**
	 * This method creates an Order message with the given parameters and sends
	 * it as an ObjectMessage to the queue.
	 * 
	 * @param orderId
	 *            ID of the product
	 * @param product
	 *            Name of the product
	 * @param price
	 *            Price of the product
	 */
	public void sendOrderToQueue(final int orderId, final String product, final BigDecimal price) {
		// TODO
		try {
			connection = connectionFactory.createConnection();
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			connection.start();
			
			destination = session.createQueue(queueName);
			producer = session.createProducer(destination);
			Order order = new Order(orderId, product, price);
			ObjectMessage message = session.createObjectMessage(order);
			message.setJMSType("Order");
			producer.send(message);
			
			connection.close();
			session.close();
		} catch (JMSException e) {
			e.printStackTrace();
		}

	}

	/**
	 * This method sends the given String to the queue as a TextMessage.
	 * 
	 * @param text
	 *            String to be sent
	 */
	public void sendTextToQueue(String text) {
		// TODO
		try {
			connection = connectionFactory.createConnection();
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			connection.start();
			
			destination = session.createQueue(queueName);
			producer = session.createProducer(destination);
			connection.start();
			TextMessage message = session.createTextMessage(text);
			producer.send(message);
			
			connection.close();
			session.close();
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Sends key-value pairs from the given map to the topic as a MapMessage.
	 * 
	 * @param map
	 *            Map of key-value pairs to be sent.
	 */
	public void sendMapToTopic(Map<String, String> map) {
		try {
			connection = connectionFactory.createConnection();
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			connection.start();

			destination = session.createQueue(queueName);
			producer = session.createProducer(destination);
			connection.start();
			MapMessage message = session.createMapMessage();
			for (Map.Entry<String, String> entry : map.entrySet()) {
				message.setString(entry.getKey(), entry.getValue());
			}
			producer.send(message);

			connection.close();
			session.close();
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}
}
