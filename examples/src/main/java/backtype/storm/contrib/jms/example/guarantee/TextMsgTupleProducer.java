package backtype.storm.contrib.jms.example.guarantee;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

import backtype.storm.contrib.jms.JmsTupleProducer;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class TextMsgTupleProducer implements JmsTupleProducer {
    
    private final String fieldName;
    
    public TextMsgTupleProducer(String fieldName) {
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
