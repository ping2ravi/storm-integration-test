package com.next.storm.topolgy.bolt;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;


//Bolt which will be tested
public class PrintBolt implements IRichBolt{
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private Logger logger = LoggerFactory.getLogger(this.getClass());


	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple input) {
		logger.info("Message Received : {}", input.getValue(0));
        collector.ack(input);
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
