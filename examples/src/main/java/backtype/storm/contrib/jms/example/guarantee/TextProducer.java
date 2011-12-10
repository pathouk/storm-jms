package backtype.storm.contrib.jms.example.guarantee;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageProducer;
import javax.jms.Session;

import backtype.storm.contrib.jms.JmsProvider;

public class TextProducer implements Runnable {
    
    private final JmsProvider jmsProvider;
    private final int numMsgs;
    
    public TextProducer(JmsProvider jmsProvider, int numMsgs) {
        this.jmsProvider = jmsProvider;
        this.numMsgs = numMsgs;
    }
    
    @Override
    public void run() {
        try {
            final ConnectionFactory factory = jmsProvider.connectionFactory();
            final Connection conn = factory.createConnection();
            final Session session = 
                    conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            final MessageProducer producer = 
                    session.createProducer(jmsProvider.destination());
            
            try {
                for (int i=1; i <= numMsgs; i++) {
                    final String text = String.format("Message #%04d", i);
                    producer.send(session.createTextMessage(text));
                    Thread.sleep(1);
                }
            } finally {
                producer.close();
                session.close();
                conn.close();
            }
        } catch (InterruptedException ok) {
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    static void produce(JmsProvider jmsProvider, int numMsgs) throws InterruptedException {
        final Thread producer = new Thread(new TextProducer(jmsProvider, numMsgs));
        try {
            producer.start();
            Thread.sleep(2000);
            producer.interrupt();
            Thread.sleep(2000);
        } finally {
            producer.interrupt();
            
            producer.join();
        }
    }
    
    public static void main(String[] args) throws Exception {
        final String brokerURL = "tcp://localhost:61616";
        final String queueName = "queue";
        produce(new ActiveMqQueueProvider(brokerURL, queueName), 100);
    }
}
