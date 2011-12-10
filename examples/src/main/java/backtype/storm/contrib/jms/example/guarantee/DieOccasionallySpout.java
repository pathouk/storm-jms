package backtype.storm.contrib.jms.example.guarantee;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;

public class DieOccasionallySpout implements IRichSpout {
    
    private final IRichSpout delegate;
    private final int interval;
    private int count;
    
    public DieOccasionallySpout(IRichSpout delegate, int interval) {
        this.delegate = delegate;
        this.interval = interval;
        this.count = 0;
    }
    
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        delegate.open(conf, context, collector);
    }
    
    @Override
    public void close() {
        delegate.close();
    }
    
    @Override
    public void nextTuple() {
        delegate.nextTuple();
    }
    
    @Override
    public void ack(Object msgId) {
        delegate.ack(msgId);
        
        // not in nextTuple because otherwise we'd die when topology is idle
        if (++count % interval == 0) {
            throw new ThreadDeath();
        }
    }
    
    @Override
    public void fail(Object msgId) {
        delegate.fail(msgId);
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        delegate.declareOutputFields(declarer);
    }
    
    @Override
    public boolean isDistributed() {
        return delegate.isDistributed();
    }
}
