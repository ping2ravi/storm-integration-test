package com.next.storm.topology.bolt;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.next.storm.integration.TestStormTopology;
import com.next.storm.integration.TestStormTopologyBuilder;

import backtype.storm.LocalCluster;
import backtype.storm.tuple.Values;

public class TestAddBoltWithTwoOutputStream {
	private static Logger logger = LoggerFactory.getLogger(TestAddBoltWithTwoOutputStream.class);
	private static LocalCluster localCluster;
	private TestStormTopology testStormTopology;
	
	@BeforeClass
	public static void startLocalClusterOnceForTest(){
        logger.info("Creating Local Cluster");
		localCluster = new LocalCluster();
		logger.info("Cluster Created");
	}
	@AfterClass
	public static void stopLocalClusterOnceForTest(){
		localCluster.shutdown();
	}
	@Before
	public void startTopology() throws Exception{
		try{
			logger.info("Starting Topology");
			TestStormTopologyBuilder testStormTopologyBuilder = new TestStormTopologyBuilder();
	        testStormTopology = testStormTopologyBuilder.buildTopology("topology-with-bolt-with-two-output.yaml", "add-bolt");

	        boolean topologyStatus = testStormTopology.startTopology(localCluster, 10, TimeUnit.SECONDS);
	        Assert.assertTrue(topologyStatus);
	        logger.info("Topology Started");
		}catch(Exception ex){
			ex.printStackTrace();
		}
        
	}
	@After
	public void stopTopology() throws Exception{
		testStormTopology.killTopology(localCluster);
	}
	
	@Test
	public void test_WhenTwoValidPositiveIntegerValuesAreSentToBolt() throws Exception{
        testStormTopology.sendMessageToStreamOfBolt("spout-1 --> bolt-1", "Test", new Values(5, 11), 5L, TimeUnit.SECONDS);
        List<Values> positiveOutput = testStormTopology.getMessageReceivedOnStream("add-bolt --> print-bolt");
        Assert.assertEquals(1, positiveOutput.size());
        Assert.assertEquals(16, positiveOutput.get(0).get(0));
        
        List<Values> negativeOutput = testStormTopology.getMessageReceivedOnStream("add-bolt --> print-negative-number-bolt");
        Assert.assertEquals(0, negativeOutput.size());
	}
	@Test
	public void test_WhenTwoValidOneNegativeAndOnePositivIntegerValuesAreSentToBolt() throws Exception{
        testStormTopology.sendMessageToStreamOfBolt("spout-1 --> bolt-1", "Test", new Values(-5, 11), 5L, TimeUnit.SECONDS);
        List<Values> positiveOutput = testStormTopology.getMessageReceivedOnStream("add-bolt --> print-bolt");
        Assert.assertEquals(1, positiveOutput.size());
        Assert.assertEquals(6, positiveOutput.get(0).get(0));
        
        List<Values> negativeOutput = testStormTopology.getMessageReceivedOnStream("add-bolt --> print-negative-number-bolt");
        Assert.assertEquals(0, negativeOutput.size());
	}
	
	@Test
	public void test_WhenMultiplePaorOfTwoValidPositiveIntegerValuesAreSentToBolt() throws Exception{
        testStormTopology.sendMessageToStreamOfBolt("spout-1 --> bolt-1", "Test01", new Values(5, 11), 5L, TimeUnit.SECONDS);
        testStormTopology.sendMessageToStreamOfBolt("spout-1 --> bolt-1", "Test02", new Values(-5, 11), 5L, TimeUnit.SECONDS);
        testStormTopology.sendMessageToStreamOfBolt("spout-1 --> bolt-1", "Test03", new Values(-105, 11), 5L, TimeUnit.SECONDS);
        testStormTopology.sendMessageToStreamOfBolt("spout-1 --> bolt-1", "Test04", new Values(13, 13), 5L, TimeUnit.SECONDS);
        testStormTopology.sendMessageToStreamOfBolt("spout-1 --> bolt-1", "Test05", new Values(0, 11), 5L, TimeUnit.SECONDS);


        List<Values> positiveOutput = testStormTopology.getMessageReceivedOnStream("add-bolt --> print-bolt");
        List<Values> negativeOutput = testStormTopology.getMessageReceivedOnStream("add-bolt --> print-negative-number-bolt");
        Assert.assertEquals(4, positiveOutput.size());
        Assert.assertEquals(16, positiveOutput.get(0).get(0));
        Assert.assertEquals(6, positiveOutput.get(1).get(0));
        Assert.assertEquals(26, positiveOutput.get(2).get(0));
        Assert.assertEquals(11, positiveOutput.get(3).get(0));

        Assert.assertEquals(1, negativeOutput.size());
        Assert.assertEquals(-94, negativeOutput.get(0).get(0));
        
	}
	
	@Test
	public void test_WhenMessageSentToAllInputStreamOfbolt() throws Exception{
        testStormTopology.sendMessageToStreamOfBolt("spout-1 --> bolt-1", "Test01", new Values(5, 11), 5L, TimeUnit.SECONDS);
        testStormTopology.sendMessageToStreamOfBolt("source-spout-two --> bolt-1", "Test02", new Values(-5, 11), 5L, TimeUnit.SECONDS);
        testStormTopology.sendMessageToStreamOfBolt("source-spout-three --> bolt-1", "Test03", new Values(-105, 11), 5L, TimeUnit.SECONDS);


        List<Values> positiveOutput = testStormTopology.getMessageReceivedOnStream("add-bolt --> print-bolt");
        List<Values> negativeOutput = testStormTopology.getMessageReceivedOnStream("add-bolt --> print-negative-number-bolt");

        Assert.assertEquals(2, positiveOutput.size());
        Assert.assertEquals(16, positiveOutput.get(0).get(0));
        Assert.assertEquals(6, positiveOutput.get(1).get(0));
        
        Assert.assertEquals(1, negativeOutput.size());
        Assert.assertEquals(-94, negativeOutput.get(0).get(0));

	}
	@Test
	public void test_WhenTwoNonIntegerValuesAreSentToBolt() throws Exception{
        testStormTopology.sendMessageToStreamOfBolt("spout-1 --> bolt-1", "Test", new Values("Notinteger", "Hello"), 5L, TimeUnit.SECONDS);
        List<Values> positiveOutput = testStormTopology.getMessageReceivedOnStream("add-bolt --> print-bolt");
        Assert.assertEquals(0, positiveOutput.size());
        
        List<Values> negativeOutput = testStormTopology.getMessageReceivedOnStream("add-bolt --> print-negative-number-bolt");
        Assert.assertEquals(0, negativeOutput.size());

	}

}
