package com.next.storm.integration.spout;

import java.util.Map;

import com.next.storm.integration.StormNotifier;
import com.next.storm.integration.queue.Message;
import com.next.storm.integration.queue.MessageQueue;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IComponent;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;

public class TestSourceSpout implements IRichSpout {

	private static final long serialVersionUID = 1L;
    private SpoutOutputCollector collector;

    private IComponent sourceComponent;
    private MessageQueue messageQueue;
    private String streamName;
    private String streamId;

    public TestSourceSpout(IComponent sourceComponent, String streamName, String streamId){
        this.sourceComponent = sourceComponent;
        this.streamName = streamName;
        this.messageQueue = MessageQueue.getInstance();
        this.streamId = streamId;
    }

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		this.messageQueue = MessageQueue.getInstance();
		StormNotifier.getInstance().sendTopologyStartedSignal();
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
        if(streamId == null){
        	System.out.println("Publishing Message to Default Stream : " + values.getMessage() +" with MessageId : "+values.getMessageId());
        	collector.emit(values.getMessage(), values.getMessageId());	
        }else{
        	System.out.println("Publishing Message to Stream : "+streamId+" with values : " + values.getMessage() +" with MessageId : "+values.getMessageId());
        	collector.emit(streamId, values.getMessage(), values.getMessageId());
        }
        
    }

	public void ack(Object msgId) {
		System.out.println("Message ACK "+ msgId);
		StormNotifier.getInstance().sendMessageProcessedSignal(msgId.toString());
	}

	public void fail(Object msgId) {
		System.out.println("Message FAIL "+ msgId);

		StormNotifier.getInstance().sendMessageProcessedSignal(msgId.toString());
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

	public String getStreamId() {
		return streamId;
	}

}
