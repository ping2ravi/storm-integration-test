package com.next.storm.topolgy.spout;

import java.util.List;
import java.util.Map;

import org.apache.storm.shade.org.eclipse.jetty.util.log.Log;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;


public class SourceSpout implements IRichSpout {

	private String addStream;
	private String outputFieldOne;
	private String outputFieldTwo;

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
		System.out.println("Declaring Fields : "+ outputFieldOne+", "+outputFieldTwo+" with stream : "+addStream);
		declarer.declareStream(addStream, new Fields("value1", "value2"));
		//declarer.declare(new Fields(outputFieldOne, outputFieldTwo));

	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	public String getAddStream() {
		return addStream;
	}

	public void setAddStream(String addStream) {
		this.addStream = addStream;
	}

	public String getOutputFieldOne() {
		return outputFieldOne;
	}

	public void setOutputFieldOne(String outputFieldOne) {
		this.outputFieldOne = outputFieldOne;
	}

	public String getOutputFieldTwo() {
		return outputFieldTwo;
	}

	public void setOutputFieldTwo(String outputFieldTwo) {
		this.outputFieldTwo = outputFieldTwo;
	}

}
