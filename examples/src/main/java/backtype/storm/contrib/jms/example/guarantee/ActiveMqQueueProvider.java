package backtype.storm.contrib.jms.example.guarantee;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;

import backtype.storm.contrib.jms.JmsProvider;

public class ActiveMqQueueProvider implements JmsProvider {
    
    private final String brokerURL;
    private final String queueName;
    
    public ActiveMqQueueProvider(String brokerURL, String queueName) {
        this.brokerURL = brokerURL;
        this.queueName = queueName;
    }
    
    @Override
    public ConnectionFactory connectionFactory() {
        return new ActiveMQConnectionFactory(brokerURL);
    }
    
    @Override
    public Destination destination() {
        return new ActiveMQQueue(queueName);
    }
}
