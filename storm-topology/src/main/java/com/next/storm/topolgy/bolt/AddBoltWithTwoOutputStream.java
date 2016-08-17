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
public class AddBoltWithTwoOutputStream implements IRichBolt{
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private Logger logger = LoggerFactory.getLogger(this.getClass());
	
	private String positiveNumberStream;
	private String negativeNumberStream;
	
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
			if(total >= 0){
				collector.emit(positiveNumberStream, input, new Values(total));
			}else{
				collector.emit(negativeNumberStream, input, new Values(total));
			}
	        collector.ack(input);
		}catch(Exception ex){
			collector.fail(input);
		}
    }

	public void cleanup() {
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(positiveNumberStream, new Fields("default"));
        declarer.declareStream(negativeNumberStream, new Fields("default"));
		
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	public String getPositiveNumberStream() {
		return positiveNumberStream;
	}

	public void setPositiveNumberStream(String positiveNumberStream) {
		this.positiveNumberStream = positiveNumberStream;
	}

	public String getNegativeNumberStream() {
		return negativeNumberStream;
	}

	public void setNegativeNumberStream(String negativeNumberStream) {
		this.negativeNumberStream = negativeNumberStream;
	}

}
