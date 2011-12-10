package backtype.storm.contrib.jms.spout;

import java.util.HashMap;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.contrib.jms.JmsProvider;
import backtype.storm.contrib.jms.JmsTupleProducer;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * A Storm <code>Spout</code> implementation that listens to a JMS topic or queue
 * and outputs tuples based on the messages it receives.
 * <p/>
 * <code>JmsSpout</code> instances rely on <code>JmsProducer</code> implementations 
 * to obtain the JMS <code>ConnectionFactory</code> and <code>Destination</code> objects
 * necessary to connect to a JMS topic/queue.
 * <p/>
 * When a <code>JmsSpout</code> receives a JMS message, it delegates to an 
 * internal <code>JmsTupleProducer</code> instance to create a Storm tuple from the 
 * incoming message.
 * <p/>
 * Typically, developers will supply a custom <code>JmsTupleProducer</code> implementation
 * appropriate for the expected message content.
 * 
 * @author P. Taylor Goetz
 *
 */
@SuppressWarnings("serial")
public class JmsSpout implements IRichSpout {
	private static final Logger LOG = LoggerFactory.getLogger(JmsSpout.class);

	// JMS options
	private int jmsAcknowledgeMode = Session.AUTO_ACKNOWLEDGE;
	
	private boolean distributed = true;

	private JmsTupleProducer tupleProducer;
	private JmsProvider jmsProvider;

	private Map<String, Message> pendingMessages;

	private SpoutOutputCollector collector;

	private transient Connection connection;
	private transient Session session;
	private transient MessageConsumer consumer;
	
	/**
	 * Sets the JMS Session acknowledgement mode for the JMS seesion associated with this spout.
	 * <p/>
	 * Possible values:
	 * <ul>
	 * <li>javax.jms.Session.AUTO_ACKNOWLEDGE</li>
	 * <li>javax.jms.Session.CLIENT_ACKNOWLEDGE</li>
	 * <li>javax.jms.Session.DUPS_OK_ACKNOWLEDGE</li>
	 * </ul>
	 * @param mode JMS Session Acknowledgement mode
	 * @throws IllegalArgumentException if the mode is not recognized.
	 */
	public void setJmsAcknowledgeMode(int mode){
		switch (mode) {
		case Session.AUTO_ACKNOWLEDGE:
		case Session.CLIENT_ACKNOWLEDGE:
		case Session.DUPS_OK_ACKNOWLEDGE:
			break;
		default:
			throw new IllegalArgumentException("Unknown Acknowledge mode: " + mode + " (See javax.jms.Session for valid values)");

		}
		this.jmsAcknowledgeMode = mode;
	}
	
	/**
	 * Returns the JMS Session acknowledgement mode for the JMS seesion associated with this spout.
	 * @return
	 */
	public int getJmsAcknowledgeMode(){
		return this.jmsAcknowledgeMode;
	}
	
	/**
	 * Set the <code>backtype.storm.contrib.jms.JmsProvider</code>
	 * implementation that this Spout will use to connect to 
	 * a JMS <code>javax.jms.Desination</code>
	 * 
	 * @param provider
	 */
	public void setJmsProvider(JmsProvider provider){
		this.jmsProvider = provider;
	}
	/**
	 * Set the <code>backtype.storm.contrib.jms.JmsTupleProducer</code>
	 * implementation that will convert <code>javax.jms.Message</code>
	 * object to <code>backtype.storm.tuple.Values</code> objects
	 * to be emitted.
	 * 
	 * @param producer
	 */
	public void setJmsTupleProducer(JmsTupleProducer producer){
		this.tupleProducer = producer;
	}

	/**
	 * <code>ISpout</code> implementation.
	 * <p/>
	 * Connects the JMS spout to the configured JMS destination
	 * topic/queue.
	 * 
	 */
	@SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		if(this.jmsProvider == null){
			throw new IllegalStateException("JMS provider has not been set.");
		}
		if(this.tupleProducer == null){
			throw new IllegalStateException("JMS Tuple Producer has not been set.");
		}
		this.pendingMessages = new HashMap<String, Message>();
		this.collector = collector;
		try {
			ConnectionFactory cf = this.jmsProvider.connectionFactory();
			Destination dest = this.jmsProvider.destination();
			this.connection = cf.createConnection();
			this.session = connection.createSession(false,
					this.jmsAcknowledgeMode);
			this.consumer = session.createConsumer(dest);
			connection.start();
		} catch (Exception e) {
			LOG.warn("Error creating JMS connection.", e);
			throw new RuntimeException(e); // so Storm knows the Spout can't run
		}
	}

	public void close() {
		try {
			LOG.debug("Closing JMS connection.");
			this.consumer.close();
			this.session.close();
			this.connection.close();
		} catch (JMSException e) {
			LOG.warn("Error closing JMS connection.", e);
		}
	}

	public void nextTuple() {
		try {
			final Message msg = this.consumer.receiveNoWait();
			if (msg == null) {
			    Utils.sleep(1);
			} else {
			    emit(msg);
			}
		} catch (JMSException e) {
			LOG.warn("Failed to receive JMS message", e);
			throw new RuntimeException(e); // so Storm knows the Spout is broken
		}
	}

	/*
	 * Will only be called if we're not AUTO_ACKNOWLEDGE
	 */
	public void ack(Object msgId) {
		Message msg = this.pendingMessages.remove(msgId);
		if (msg != null) {
			try {
				msg.acknowledge();
				LOG.debug("JMS Message acked: {}", msgId);
			} catch (JMSException e) {
				LOG.warn("Error acknowldging JMS message: " + msgId, e);
				throw new RuntimeException(e); // so Storm knows the Spout is broken
			}
		} else {
			LOG.warn("Couldn't acknowledge unknown JMS message ID: {}", msgId);
		}
	}

	/*
	 * Will only be called if we're not AUTO_ACKNOWLEDGE
	 */
	public void fail(Object msgId) {
		LOG.debug("Message failed: {}", msgId);
		Message msg = this.pendingMessages.remove(msgId);
		if (msg != null) {
			emit(msg);
		} else {
			LOG.warn("Couldn't fail unknown JMS message ID: {}", msgId);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		this.tupleProducer.declareOutputFields(declarer);
	}

	public boolean isDistributed() {
		return this.distributed;
	}
	
	/**
	 * Sets the "distributed" mode of this spout.
	 * <p/>
	 * If <code>true</code> multiple instances of this spout <i>may</i> be
	 * created across the cluster (depending on the "parallelism_hint" in the topology configuration).
	 * <p/>
	 * Setting this value to <code>false</code> essentially means this spout will run as a singleton 
	 * within the cluster ("parallelism_hint" will be ignored).
	 * <p/>
	 * In general, this should be set to <code>false</code> if the underlying JMS destination is a 
	 * topic, and <code>true</code> if it is a JMS queue.
	 * 
	 * @param distributed
	 */
	public void setDistributed(boolean distributed){
		this.distributed = distributed;
	}

	private void emit(Message msg) {
		LOG.debug("sending tuple: {}", msg);
		// get the tuple from the handler
		try {
			Values vals = this.tupleProducer.toTuple(msg);
			// ack if we're not in AUTO_ACKNOWLEDGE mode, or the message
			// requests ACKNOWLEDGE
			if (LOG.isDebugEnabled()) {
				LOG.debug("Requested deliveryMode: " + toDeliveryModeString(msg.getJMSDeliveryMode()));
				LOG.debug("Our deliveryMode: " + toDeliveryModeString(this.jmsAcknowledgeMode));
			}
			if (this.jmsAcknowledgeMode != Session.AUTO_ACKNOWLEDGE
				|| msg.getJMSDeliveryMode() != Session.AUTO_ACKNOWLEDGE) {
				LOG.debug("Requesting acks.");
				this.collector.emit(vals, msg.getJMSMessageID());

				// at this point we successfully emitted. Store
				// the message and message ID so we can do a
				// JMS acknowledge later
				this.pendingMessages.put(msg.getJMSMessageID(), msg);
			} else {
				this.collector.emit(vals);
			}
		} catch (JMSException e) {
			LOG.warn("Unable to convert JMS message: {}", msg);
		}
	}

	private static final String toDeliveryModeString(int deliveryMode) {
		switch (deliveryMode) {
		case Session.AUTO_ACKNOWLEDGE:
			return "AUTO_ACKNOWLEDGE";
		case Session.CLIENT_ACKNOWLEDGE:
			return "CLIENT_ACKNOWLEDGE";
		case Session.DUPS_OK_ACKNOWLEDGE:
			return "DUPS_OK_ACKNOWLEDGE";
		default:
			return "UNKNOWN";
		}
	}
}
