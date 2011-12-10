package backtype.storm.contrib.jms.example;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.contrib.jms.JmsProvider;
import backtype.storm.contrib.jms.JmsTupleProducer;
import backtype.storm.contrib.jms.spout.JmsSpout;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class JmsGuaranteeTopology {
    
    private static class ActiveMqQueueProvider implements JmsProvider {
        private final String brokerURL;
        private final String queueName;
        
        ActiveMqQueueProvider(String brokerURL, String queueName) {
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
    private static class TextMsgTupleProducer implements JmsTupleProducer {
        private final String fieldName;
        
        TextMsgTupleProducer(String fieldName) {
            this.fieldName = fieldName;
        }
        @Override
        public Values toTuple(Message msg) throws JMSException {
            return (msg instanceof TextMessage) 
                ? new Values(((TextMessage) msg).getText())
                : null;
        }
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(fieldName));
        }
    }
    
    private static class FailEachTupleOnceBolt implements IRichBolt {
        private final String fieldName;
        private final Set<Object> fails = new HashSet<Object>();
        private OutputCollector collector;
        
        FailEachTupleOnceBolt(String fieldName) {
            this.fieldName = fieldName;
        }
        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }
        @Override
        public void execute(Tuple input) {
            final Object value = input.getValueByField(fieldName);
            if (fails.remove(value)) {
                collector.emit(input, new Values(value));
                collector.ack(input);
            } else {
                fails.add(value);
                collector.fail(input);
            }
        }
        @Override
        public void cleanup() {
        }
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(fieldName));
        }
    }
    private static class CollectorBolt implements IRichBolt {
        private final String fieldName;
        private final Set<Object> values = new TreeSet<Object>(); // sorted
        private OutputCollector collector;
        
        CollectorBolt(String fieldName) {
            this.fieldName = fieldName;
        }
        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }
        @Override
        public void execute(Tuple input) {
            values.add(input.getValueByField(fieldName));
            collector.ack(input);
        }
        @Override
        public void cleanup() {
            System.out.println("----- FINAL VALUES -----");
            for (Object v : values) {
                System.out.println(v);
            }
            System.out.println("------------------------");
        }
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields());
        }
    }
    
    private static BrokerService startEmbeddedBroker(String url) throws Exception {
        final BrokerService broker = new BrokerService();
        broker.addConnector(url);
        broker.start();
        return broker;
    }
    
    private static class TextProducer implements Runnable {
        private final JmsProvider jmsProvider;
        private final int numMsgs;
        
        TextProducer(JmsProvider jmsProvider, int numMsgs) {
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
                
                for (int i=1; i <= numMsgs; i++) {
                    final String text = String.format("Message #%04d", i);
                    producer.send(session.createTextMessage(text));
                    Thread.sleep(1);
                }
            } catch (InterruptedException ok) {
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    
    public static void main(String[] args) throws Exception {
        final String brokerURL = "tcp://localhost:61616";
        final String queueName = "queue";
        final String fieldName = "text";
        final JmsProvider jmsProvider = new ActiveMqQueueProvider(brokerURL, queueName);
        
        final JmsSpout queueSpout = new JmsSpout();
        queueSpout.setJmsProvider(jmsProvider);
        queueSpout.setJmsTupleProducer(new TextMsgTupleProducer(fieldName));
        queueSpout.setJmsAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
        
        final TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", queueSpout, 1);
        builder.setBolt("failer", new FailEachTupleOnceBolt(fieldName), 3)
               .fieldsGrouping("spout", new Fields(fieldName));
        builder.setBolt("final", new CollectorBolt(fieldName), 1)
               .fieldsGrouping("failer", new Fields(fieldName));
        
        final Config conf = new Config();
        if (args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
            conf.setDebug(true);
            final LocalCluster cluster = new LocalCluster();
            final BrokerService broker = startEmbeddedBroker(brokerURL);
            final Thread producer = new Thread(new TextProducer(jmsProvider, 100));
            try {
                cluster.submitTopology("guarantee", conf, builder.createTopology());
                producer.start();
                Thread.sleep(2000);
                producer.interrupt();
                Thread.sleep(2000);
            } finally {
                cluster.killTopology("guarantee");
                cluster.shutdown();
                producer.interrupt();
                producer.join();
                broker.stop();
            }
        }
    }
}
