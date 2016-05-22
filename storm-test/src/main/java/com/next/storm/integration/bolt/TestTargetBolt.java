package com.next.storm.integration.bolt;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.next.storm.integration.queue.Message;
import com.next.storm.integration.queue.MessageQueue;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TestTargetBolt implements IRichBolt{

	private static final long serialVersionUID = 1L;
	private String streamName;
	private OutputCollector collector;
	private Logger logger = LoggerFactory.getLogger(this.getClass());
	
	public TestTargetBolt(String streamName){
		this.streamName = streamName;
	}

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple input) {
		logger.info("Mesage Received by Target Bolt {}", input);
		MessageQueue messageQueue = MessageQueue.getInstance();
		Message message = new Message();
		Fields fields = input.getFields();
		Values values = new Values();
		for(String oneField:fields){
			values.add(input.getValueByField(oneField));
			logger.info("   {} : {}", oneField, input.getValueByField(oneField));
		}
		message.setMessage(values);
		messageQueue.addMessageToQueue(streamName, message);
        collector.ack(input);

	}

	public void cleanup() {
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	public String getStreamName() {
		return streamName;
	}

}
