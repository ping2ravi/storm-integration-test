package com.next.storm.topolgy.bolt;

import java.util.Map;

import backtype.storm.task.IBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

//Bolt which will be tested
public class AddBolt implements IRichBolt{

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		
	}

	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		
	}

	public void cleanup() {
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
