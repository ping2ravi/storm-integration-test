package com.next.storm.integration;

import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IComponent;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Values;

import org.apache.storm.flux.model.BeanDef;
import org.apache.storm.flux.model.BoltDef;
import org.apache.storm.flux.model.GroupingDef;
import org.apache.storm.flux.model.PropertyDef;
import org.apache.storm.flux.model.SpoutDef;
import org.apache.storm.flux.model.StreamDef;
import org.apache.storm.flux.model.TopologyDef;
import org.apache.storm.flux.parser.FluxParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import com.next.storm.integration.bolt.TestTargetBolt;
import com.next.storm.integration.queue.MessageQueue;
import com.next.storm.integration.spout.TestSourceSpout;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TestStormTopologyBuilder {
	private Logger logger = LoggerFactory.getLogger(this.getClass());
	public static void main(String[] args) throws Exception {
        System.out.println("Starting Local Cluster "+ new Date());
        LocalCluster localCluster = new LocalCluster();
        System.out.println("Local Cluster Started "+ new Date());
        try{
            TestStormTopologyBuilder testStormTopologyBuilder = new TestStormTopologyBuilder();
            TestStormTopology testStormTopology = testStormTopologyBuilder.buildTopology("test-topology.yaml", "add-bolt");

            testStormTopology.startTopology(localCluster, 10, TimeUnit.SECONDS);
            System.out.println("Topology Started " + new Date());
            testStormTopology.sendMessageToStreamOfBolt("spout-1 --> bolt-1", "Test", new Values("Hello Testing World : "+ new Date()), 5L, TimeUnit.SECONDS);
            List<Values> output = testStormTopology.getMessageReceivedOnStream("add-bolt --> print-bolt");
            System.out.println("Total Message Received :"+output.size());
            for(Values oneMessage :output){
            	System.out.println(" oneMessage "+oneMessage.get(0));
            }

        }finally{
            localCluster.shutdown();
        }
        System.out.println("Cluster shut down successfully " + new Date());
	}

	public TestStormTopology buildTopology(String fluxTopologyFileName, final String boltName) throws Exception {
		Resource resource = new ClassPathResource(fluxTopologyFileName);
		TopologyDef topologyDef = FluxParser.parseFile(resource.getFile().getAbsolutePath(), false, false, null, false);
		IRichBolt boltBeingTested = buildBolt(topologyDef, boltName);

		Map<String, TestSourceSpout> spoutStreams = buildInputStreams(topologyDef, boltName, boltBeingTested);
		Map<String, TestTargetBolt> boltOutputStreams = buildOutputStreams(topologyDef, boltName, boltBeingTested);

        TopologyBuilder builder = new TopologyBuilder();
        BoltDeclarer boltDeclarer =  builder.setBolt(boltName, boltBeingTested, 1);
        int count = 0;
        String spoutName;
        for(TestSourceSpout oneIRichSpout : spoutStreams.values()){
            spoutName = "SPOUT-"+count;
            builder.setSpout(spoutName, oneIRichSpout, 1);
            if(oneIRichSpout.getStreamId() == null){
            	logger.info("No Stream Id defined so using default");
            	boltDeclarer.shuffleGrouping(spoutName);	
            }else{
            	logger.info("Stream Id defined so using {}", oneIRichSpout.getStreamId());
            	boltDeclarer.shuffleGrouping(spoutName, oneIRichSpout.getStreamId());
            }
            
            logger.info("Spout Created : {}, for input stream {} ",spoutName, oneIRichSpout.getStreamName());
            count++;
        }
        count = 0;
        String newBoltName;
        for(TestTargetBolt oneIRichBolt : boltOutputStreams.values()){
        	newBoltName = "BOLT-"+count;
        	boltDeclarer = builder.setBolt(newBoltName, oneIRichBolt, 1);
            boltDeclarer.shuffleGrouping(boltName, oneIRichBolt.getStreamId());
            logger.info("Bolt Created : {}, for output stream {} ",newBoltName, oneIRichBolt.getStreamName());
            count++;
        }

        StormTopology stormTopology = builder.createTopology();
        TestStormTopologyImpl testStormTopology = new TestStormTopologyImpl(stormTopology, spoutStreams, boltOutputStreams);
        return testStormTopology;
	}
    private Map<String, TestSourceSpout> buildInputStreams(TopologyDef topologyDef, String boltName, IRichBolt boltBeingTestes) throws Exception {
        List<StreamDef> streams = topologyDef.getStreams();
        Map<String, TestSourceSpout> spoutStreams = new HashMap<>();

        for(StreamDef oneStreamDef : streams){
            if(oneStreamDef.getTo().equals(boltName)){
                if(!GroupingDef.Type.SHUFFLE.equals(oneStreamDef.getGrouping().getType())){
                    logger.error("Only SHuffle Grouping is supported for now. Keep an eye for update or feel free to send pull request");
                    throw new RuntimeException("Only SHuffle Grouping is supported for now. Keep an eye for update or feel free to send pull request");
                }
                MessageQueue.getInstance().createMessageStream(oneStreamDef.getName());
                IComponent sourceCompanent = buildComponent(topologyDef, oneStreamDef.getFrom());
                String streamId = oneStreamDef.getGrouping().getStreamId();
                if(streamId == null || streamId.trim().equals("")){
        			streamId = "default";	
        		}
                TestSourceSpout spout = new TestSourceSpout(sourceCompanent, oneStreamDef.getName(), streamId);
                spoutStreams.put(oneStreamDef.getName(), spout);
            }
        }
        logger.info("Total Spouts Created= {}",spoutStreams.size());
        return spoutStreams;
    }
    private Map<String, TestTargetBolt> buildOutputStreams(TopologyDef topologyDef, String boltName, IRichBolt boltBeingTestes) throws Exception {
        List<StreamDef> streams = topologyDef.getStreams();
        Map<String, TestTargetBolt> boltOutputStreams = new HashMap<>();

        for(StreamDef oneStreamDef : streams){
            if(oneStreamDef.getFrom().equals(boltName)){
                if(!GroupingDef.Type.SHUFFLE.equals(oneStreamDef.getGrouping().getType())){
                    System.err.println("Only SHuffle Grouping is supported for now. Keep an eye for update or feel free to send pull request");
                }
                MessageQueue.getInstance().createMessageStream(oneStreamDef.getName());
                String streamId = oneStreamDef.getGrouping().getStreamId();
                if(streamId == null || streamId.trim().equals("")){
        			streamId = "default";	
        		}
                TestTargetBolt spout = new TestTargetBolt(oneStreamDef.getName(), streamId);
                boltOutputStreams.put(oneStreamDef.getName(), spout);
            }
        }
        logger.info("Total boltOutputStreams = {}", boltOutputStreams.size());
        return boltOutputStreams;
    }
    private IComponent buildComponent(TopologyDef topologyDef, String refName) throws Exception{
		BoltDef boltDef = topologyDef.getBoltDef(refName);
		if(boltDef != null){
			return buildBolt(topologyDef, refName);
		}
		SpoutDef spoutDef = topologyDef.getSpoutDef(refName);
		if(spoutDef != null){
			return buildSpout(topologyDef, refName);
		}
		return null;
    }
	private Object buildObject(TopologyDef topologyDef, String className, List<PropertyDef> properties)
			throws Exception {
		Class cls = Class.forName(className);
		Object object = cls.newInstance();
		if (properties == null) {
			return object;
		}
		for (PropertyDef onePropertyDef : properties) {
			logger.debug("PropertyName : {}, Value :{}, Ref : {}", onePropertyDef.getName() , onePropertyDef.getValue(), onePropertyDef.getRef());
			if (onePropertyDef.getValue() == null) {
				setProperty(object, onePropertyDef.getName(), buildRef(topologyDef, onePropertyDef.getRef()));
			} else {
				setProperty(object, onePropertyDef.getName(), onePropertyDef.getValue());
			}
		}
		return object;
	}

	private IRichBolt buildBolt(TopologyDef topologyDef, String boltName) throws Exception {
		BoltDef boltDef = topologyDef.getBoltDef(boltName);
		String className = boltDef.getClassName();

		return (IRichBolt) buildObject(topologyDef, className, boltDef.getProperties());
	}
	private IRichSpout buildSpout(TopologyDef topologyDef, String spoutName) throws Exception {
		SpoutDef spoutDef = topologyDef.getSpoutDef(spoutName);
		String className = spoutDef.getClassName();

		return (IRichSpout) buildObject(topologyDef, className, spoutDef.getProperties());
	}

	private Object buildRef(TopologyDef topologyDef, String refName) throws Exception {
		BeanDef beanDef = topologyDef.getComponent(refName);
		String className = beanDef.getClassName();
		return buildObject(topologyDef, className, beanDef.getProperties());
	}

	private void setProperty(Object obj, String fieldName, Object fieldValue)
			throws InvocationTargetException, IllegalAccessException {
		Method[] methods = obj.getClass().getMethods();
		String methodName = "set" + Character.toUpperCase(fieldName.toCharArray()[0]) + fieldName.substring(1);
		logger.debug("Method to lookup : {}", methodName);
		for (Method oneMethod : methods) {
			if (oneMethod.getName().equals(methodName)) {
				oneMethod.invoke(obj, fieldValue);
			}
		}
	}

}
