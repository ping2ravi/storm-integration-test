package com.next.storm.integration.spout;

import java.util.Map;

import com.next.storm.integration.StormNotifier;
import com.next.storm.integration.queue.Message;
import com.next.storm.integration.queue.MessageQueue;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IComponent;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Values;

public class TestSourceSpout implements IRichSpout{

	private static final long serialVersionUID = 1L;
    private SpoutOutputCollector collector;

    private IComponent sourceComponent;
    private MessageQueue messageQueue;
    private String streamName;

    public TestSourceSpout(IComponent sourceComponent, String streamName){
        this.sourceComponent = sourceComponent;
        this.streamName = streamName;
        this.messageQueue = MessageQueue.getInstance();
    }

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		this.messageQueue = MessageQueue.getInstance();
		StormNotifier.getInstance().sendProcessedSignal("StartTopology");
	}

	public void close() {
		
	}

	public void activate() {
	}

	public void deactivate() {
	}
	
	public void sendMessage(Message message){
		messageQueue.addMessageToQueue(streamName, message);
	}

	public void nextTuple() {
        Message values = messageQueue.getMessageFromQueue(streamName);
        if(values == null){
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return;
        }
        System.out.println("**** NEXT TUPLE "+values);
        collector.emit(values.getMessage(), values.getMessageId());
    }

	public void ack(Object msgId) {
		System.out.println("Message ACK "+ msgId);
		StormNotifier.getInstance().sendProcessedSignal(msgId.toString());
	}

	public void fail(Object msgId) {
		System.out.println("Message FAIL "+ msgId);

		StormNotifier.getInstance().sendProcessedSignal(msgId.toString());		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
        sourceComponent.declareOutputFields(declarer);
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	public String getStreamName() {
		return streamName;
	}

}
