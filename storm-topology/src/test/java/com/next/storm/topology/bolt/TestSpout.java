package com.next.storm.topology.bolt;

import com.next.storm.integration.TestStormTopology;
import com.next.storm.integration.TestStormTopologyBuilder;
import org.apache.storm.LocalCluster;
import org.apache.storm.tuple.Values;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class TestSpout {
    private static Logger logger = LoggerFactory.getLogger(TestAddBolt.class);
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
            testStormTopology = testStormTopologyBuilder.buildTopology("test-topology-spout.yaml", "timer-spout");

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
        //insert record in DB
        boolean messageEmitted = testStormTopology.waitUntilNMessagesAreProcessed(3, 1);
        Assert.assertTrue(messageEmitted);
        //Thread.sleep(1000);

        List<Values> output = testStormTopology.getMessageReceivedOnStream("timer-spout --> print-bolt");
        Assert.assertEquals(1, output.size());
        Assert.assertEquals("Tick", output.get(0).get(0));
        List<Values> output2 = testStormTopology.getMessageReceivedOnStream("timer-spout --> print-bolt2");
        Assert.assertEquals(2, output2.size());
        Assert.assertEquals("Tick2", output2.get(0).get(0));
    }}
