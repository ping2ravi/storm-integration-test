package com.next.storm.topolgy.bolt;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

//Bolt which will be tested
public class AddBolt implements IRichBolt{
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private Logger logger = LoggerFactory.getLogger(this.getClass());
	
	
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple input) {
		try{
			int inputOne = input.getInteger(0);
			int inputTwo = input.getInteger(1);

			logger.info("Message Received inputOne"+ inputOne);
			logger.info("Message Received inputTwo"+ inputTwo);
			
			int total = inputOne + inputTwo;
	        collector.emit(input, new Values(total));
	        collector.ack(input);
		}catch(Exception ex){
			collector.fail(input);
		}
    }

	public void cleanup() {
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("default", new Fields("default"));
		
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
