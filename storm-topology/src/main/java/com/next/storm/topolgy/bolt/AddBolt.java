package com.next.storm.topolgy.bolt;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//Bolt which will be tested
public class AddBolt implements IRichBolt {
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
