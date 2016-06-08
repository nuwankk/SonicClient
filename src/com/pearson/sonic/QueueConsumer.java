package com.pearson.sonic;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;





import progress.message.jclient.TopicConnectionFactory;

public class QueueConsumer {

	private static Connection getConnection() {

		String brokerUrl ="10.52.122.127:2507,10.52.122.174:2507"; 
		
		Connection connection = null;

		try {
			TopicConnectionFactory factory;
			factory = (new progress.message.jclient.TopicConnectionFactory());

			factory.setConnectionURLs(brokerUrl);
			//factory.setAsynchronousDeliveryMode();
			
			
			connection = factory.createConnection("rumba_dev", "rumba_dev");
			
						
		} catch (JMSException e) {
			e.printStackTrace();
		}
		return connection;
	}

	public static void receiveMessage() {
		Connection connection = getConnection();
		Session receiveSession;

		         String queue = "QREC1";

		try {
			receiveSession = connection.createSession(false,
					javax.jms.Session.CLIENT_ACKNOWLEDGE);
			
			Queue receiveQueue = receiveSession.createQueue(queue);
			
			MessageConsumer mcon = receiveSession.createConsumer(receiveQueue);
			
			connection.start();
			Message message = null;
			
			for (int i = 0; i > -1; i++) {
				long start_time = System.nanoTime();
				message = mcon.receiveNoWait();
				
				long end_time = System.nanoTime();
				System.out.println("Time in millis : "+(end_time - start_time)/1e6);
				TextMessage receiveMsg = (TextMessage) message;
				receiveMsg.clearBody();
			}
			
			

			mcon.close();
			receiveSession.close();
			connection.close();

		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}