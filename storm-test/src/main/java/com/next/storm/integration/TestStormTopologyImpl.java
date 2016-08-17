package com.next.storm.integration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.storm.LocalCluster;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.StormTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import com.next.storm.integration.bolt.TestTargetBolt;
import com.next.storm.integration.queue.Message;
import com.next.storm.integration.queue.MessageQueue;
import com.next.storm.integration.spout.TestSourceSpout;


import org.apache.storm.tuple.Values;

public class TestStormTopologyImpl implements TestStormTopology {
	private Logger logger =  LoggerFactory.getLogger(this.getClass());
    private StormTopology stormTopology;
    private Map<String, TestSourceSpout> spoutStreams;
    private Map<String, TestTargetBolt> boltOutputStreams;
    private final String topologyName = "test-topology";
    public TestStormTopologyImpl(StormTopology stormTopology, Map<String, TestSourceSpout> spoutStreams, Map<String, TestTargetBolt> boltOutputStreams){
        this.stormTopology = stormTopology;
        this.spoutStreams = spoutStreams;
        this.boltOutputStreams = boltOutputStreams;
    }

    @Override
	public boolean startTopology(LocalCluster localCluster, long timeOut, TimeUnit timeUnit) throws Exception{
    	StormNotifier.getInstance().startNotificationFlow("StartTopology");
        localCluster.submitTopology(topologyName, new HashMap(), stormTopology);
        return StormNotifier.getInstance().waitForNotification("StartTopology", timeOut, timeUnit);
	}

    @Override
	public boolean sendMessageToStreamOfBolt(String streamId, String messageId, Values message, long timeout, TimeUnit timeUnit) throws Exception {
    	TestSourceSpout spout = spoutStreams.get(streamId);
    	if(spout == null){
    		throw new RuntimeException("No Stream Found "+streamId+", Available streams are "+StringUtils.collectionToDelimitedString(spoutStreams.keySet(), ",","[","]"));
    	}
    	StormNotifier.getInstance().startNotificationFlow(messageId);
    	Message messageWithId = new Message();
    	messageWithId.setMessage(message);
    	messageWithId.setMessageId(messageId);
    	spout.sendMessage(messageWithId);
    	return StormNotifier.getInstance().waitForNotification(messageId, timeout, timeUnit);
	}

	@Override
	public List<Values> getMessageReceivedOnStream(String streamId) throws Exception {
		List<Message> mesages = MessageQueue.getInstance().getMessageQueue(streamId);
		List<Values> values = new ArrayList<>();
		for(Message oneMessage : mesages){
			values.add(oneMessage.getMessage());
		}
		return values;
	}

	@Override
	public boolean killTopology(LocalCluster localCluster) throws Exception{
		KillOptions killOptions = new KillOptions();
		killOptions.set_wait_secs(1);
		localCluster.killTopologyWithOpts(topologyName, killOptions);
		try {//Give time to topology to be killed
			Thread.sleep(1200);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		try{
			localCluster.getTopologyInfo(topologyName);
			return false;
		}catch(Exception ex){
			return true;
		}
	}

}
