package com.next.storm.integration.spout;

import com.next.storm.integration.StormNotifier;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;

import java.util.Map;

public class TestProxySpout implements IRichSpout {

	private static final long serialVersionUID = 1L;

    private IRichSpout spoutToTest;

    public TestProxySpout(IRichSpout spoutToTest){
        this.spoutToTest = spoutToTest;
    }

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		spoutToTest.open(conf, context, collector);
		StormNotifier.getInstance().sendTopologyStartedSignal();
	}

	public void close() {
		spoutToTest.close();
	}

	public void activate() {
		spoutToTest.activate();
	}

	public void deactivate() {
		spoutToTest.deactivate();
	}
	


	public void nextTuple() {
		System.out.println("Calling next Tuple");
		spoutToTest.nextTuple();
    }

	public void ack(Object msgId) {
		try{
			spoutToTest.ack(msgId);
		}catch(Exception ex){
			ex.printStackTrace();
		}
		System.out.println("Message ACK "+ msgId);
		StormNotifier.getInstance().sendMessageProcessedSignal(msgId.toString());
	}

	public void fail(Object msgId) {
		try{
			spoutToTest.fail(msgId);
		}catch(Exception ex){
			ex.printStackTrace();
		}
		System.out.println("Message FAIL "+ msgId);
		StormNotifier.getInstance().sendMessageProcessedSignal(msgId.toString());
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		spoutToTest.declareOutputFields(declarer);
	}

	public Map<String, Object> getComponentConfiguration() {
		return spoutToTest.getComponentConfiguration();
	}
}
