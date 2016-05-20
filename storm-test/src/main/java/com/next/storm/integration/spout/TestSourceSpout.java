package com.next.storm.integration.spout;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;

public class TestSourceSpout implements IRichSpout{

	private static final long serialVersionUID = 1L;

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		
	}

	public void close() {
		// TODO Auto-generated method stub
		
	}

	public void activate() {
		// TODO Auto-generated method stub
		
	}

	public void deactivate() {
		// TODO Auto-generated method stub
		
	}

	public void nextTuple() {
		// TODO Auto-generated method stub
		
	}

	public void ack(Object msgId) {
		// TODO Auto-generated method stub
		
	}

	public void fail(Object msgId) {
		// TODO Auto-generated method stub
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
