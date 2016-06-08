package com.pearson.sonic;

import javax.jms.*;

import progress.message.jclient.TopicConnectionFactory;

public class QueueProducer {

	private static Connection getConnection() {
		
		String brokerUrl ="10.52.122.127:2507,10.52.122.174:2507"; 
		
		Connection connection = null;

		try {
			TopicConnectionFactory factory;
			factory = (new progress.message.jclient.TopicConnectionFactory());

			factory.setConnectionURLs(brokerUrl);
			factory.setAsynchronousDeliveryMode(1);
			
			connection = factory.createConnection("rumba_dev", "rumba_dev");
						
		} catch (JMSException e) {
			e.printStackTrace();
		}
		return connection;
	}

	public static void sendMessage(int messageCount) throws InterruptedException {
		Connection connection = getConnection();
		Session sendSession;
		String queue = "queue.pearson.ed.rumba.request.notification.soap";
		
		try {
			
			sendSession = connection.createSession(false,
					javax.jms.Session.CLIENT_ACKNOWLEDGE);
			
			System.out.println(connection.getMetaData());
			Queue sendQueue = sendSession.createQueue(queue);
			
			MessageProducer mp = sendSession.createProducer(sendQueue);
			TextMessage sendMsg = sendSession.createTextMessage("messagemessagemessagemessagemessagemessagemessagemessagemessagemessa"
					+ "gemessagemessagemessagemessagemessagemessagemessagemessagemessageme");
			
			mp.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
			
			
			for (int i = 0; i < messageCount; i++) {
				long start_time = System.nanoTime();
				mp.send(sendMsg);
				long end_time = System.nanoTime();
				double difference = (end_time - start_time)/1e6;
				System.out.println("Time in millis : "+ difference);
			}

			mp.close();
			sendSession.close();
			connection.close();
		} catch (JMSException e) {
			e.printStackTrace();
		}

	}
	
}
