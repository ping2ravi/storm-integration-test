package com.next.storm.topolgy.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;


public class TimerSpout implements IRichSpout {

	private String outputStream;
	private String outputStream2;
	private String outputField;

	private SpoutOutputCollector collector;
	boolean active =true;


	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
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
		if(active){
			collector.emit(outputStream, new Values("Tick"), "msg1");
			collector.emit(outputStream2, new Values("Tick2"), "msg2");
			collector.emit(outputStream2, new Values("Tick3"), "msg3");
		}
		active = false;
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

	public void ack(Object msgId) {
		// TODO Auto-generated method stub

	}

	public void fail(Object msgId) {
		// TODO Auto-generated method stub

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		System.out.println("Declaring Fields : "+ outputField+" with stream : "+outputStream);
		declarer.declareStream(outputStream, new Fields(outputField));
		System.out.println("Declaring Fields : "+ outputField+" with stream : "+outputStream2);
		declarer.declareStream(outputStream2, new Fields(outputField));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	public String getOutputField() {
		return outputField;
	}

	public void setOutputField(String outputField) {
		this.outputField = outputField;
	}

	public String getOutputStream() {
		return outputStream;
	}

	public void setOutputStream(String outputStream) {
		this.outputStream = outputStream;
	}

	public String getOutputStream2() {
		return outputStream2;
	}

	public void setOutputStream2(String outputStream2) {
		this.outputStream2 = outputStream2;
	}

}
