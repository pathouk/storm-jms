package backtype.storm.contrib.jms.example.guarantee;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import javax.jms.Session;

import org.apache.activemq.broker.BrokerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.contrib.jms.JmsProvider;
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
        
        private static final Logger log = LoggerFactory.getLogger(CollectorBolt.class);
        
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
            
            if (values.size() % 25 == 0) {
                dumpValues();
            }
        }
        
        @Override
        public void cleanup() {
            dumpValues();
        }
        
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields());
        }
        
        private void dumpValues() {
            log.info("----- VALUES ({}) -----", values.size());
            for (Object v : values) {
                log.info("VALUE: {}", v);
            }
        }
    }
    
    private static BrokerService startEmbeddedBroker(String url) throws Exception {
        final BrokerService broker = new BrokerService();
        broker.addConnector(url);
        broker.start();
        return broker;
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
        builder.setSpout("spout", new DieOccasionallySpout(queueSpout, 10), 2);
//        builder.setSpout("spout", queueSpout, 1);
        builder.setBolt("failer", new FailEachTupleOnceBolt(fieldName), 3)
               .fieldsGrouping("spout", new Fields(fieldName));
        builder.setBolt("final", new CollectorBolt(fieldName), 1)
               .fieldsGrouping("failer", new Fields(fieldName));
        
        final Config conf = new Config();
        if (args.length > 0) {
            conf.setNumWorkers(2);
            conf.setMessageTimeoutSecs(5);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
            conf.setDebug(true);
            final LocalCluster cluster = new LocalCluster();
            final BrokerService broker = startEmbeddedBroker(brokerURL);
            try {
                cluster.submitTopology("guarantee", conf, builder.createTopology());
                TextProducer.produce(jmsProvider, 100);
            } finally {
                cluster.killTopology("guarantee");
                cluster.shutdown();
                broker.stop();
            }
        }
    }
}
