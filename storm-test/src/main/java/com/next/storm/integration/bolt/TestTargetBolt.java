package com.next.storm.integration.bolt;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.next.storm.integration.queue.Message;
import com.next.storm.integration.queue.MessageQueue;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class TestTargetBolt implements IRichBolt{

	private static final long serialVersionUID = 1L;
	private String streamName;
	private String streamId;
	private OutputCollector collector;
	private Logger logger = LoggerFactory.getLogger(this.getClass());
	
	public TestTargetBolt(String streamName, String streamId){
		this.streamName = streamName;
		this.streamId = streamId;
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

	public String getStreamId() {
		return streamId;
	}

}
