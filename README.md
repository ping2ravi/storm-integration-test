# storm-integration-test

A Test framework to do integration test for your Storm bolts

## How does it work(Short Version)?
Maven User include this in your dependecy section

```xml
<dependency>
	<groupId>com.next.storm.integration</groupId>
	<artifactId>storm-test</artifactId>
	<version>0.0.1</version>
</dependency>

<repository>
    <id>storm-integration-test</id>
    <name>Storm Integration Test Repository</name>
    <url>https://mymavenrepo.com/repo/1RZXbxY8pGEBRbvs3paa/</url>
</repository>

```

Lets assume you have this flux file([copied from storm-topology project](https://github.com/ping2ravi/storm-integration-test/blob/master/storm-topology/src/main/resources/test-topology.yaml))

```yml
name: "test-topology"
config:
  topology.workers: 1

# spout definitions
spouts:
  - id: "source-spout"
    className: "com.next.storm.topolgy.spout.SourceSpout"
    parallelism: 1

# bolt definitions
bolts:
  - id: "add-bolt"
    className: "com.next.storm.topolgy.bolt.AddBolt"
    parallelism: 1
  - id: "print-bolt"
    className: "com.next.storm.topolgy.bolt.PrintBolt"
    parallelism: 1

#stream definitions
streams:
  - name: "spout-1 --> bolt-1"
    from: "source-spout"
    to: "add-bolt"
    grouping:
      type: SHUFFLE
  - name: "add-bolt --> print-bolt"
    from: "add-bolt"
    to: "print-bolt"
    grouping:
      type: SHUFFLE     

```

Its a very simple topology where spout send two values in atuple, Add Bolt add them and send result to Print Bolt, which just prints it. In this example we are plannign to test AddBolt, so implementaion of Spout and Print Bolt doesnt matter, only thing matters is that AddBolt is listening to streams from Spout and writing to Stream which is listened by Print Bolt. Basically Add Bolt has input and output stream

### Test Strategy

So what do we want to test?, we want to test the whole functionality of Add Bolt(Integration test) and to do that we need to send to Tuple to Input Stream(s) of Add Bolt and then Listen to Output stream of AddBolt to get the result.


Define a LocalCluster in your Test class to run a topology and create Starte and Stop Methods

```java
	private static LocalCluster localCluster;
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
```

I am creating it as static and using @BeforeClass,@ Afterclass as Creating LocalCluster is bit time consuming, so better to create it only once(much better if you can create it only once for the whole test suite ratehrn then just class)


Now Lets create a [TesStormTopology](https://github.com/ping2ravi/storm-integration-test/blob/master/storm-test/src/main/java/com/next/storm/integration/TestStormTopology.java) using [TestStormTopologyBuilder](https://github.com/ping2ravi/storm-integration-test/blob/master/storm-test/src/main/java/com/next/storm/integration/TestStormTopologyBuilder.java)

```java
  private TestStormTopology testStormTopology;//Note : Its not Static variable 

	@Before
	public void startTopology() throws Exception{
        logger.info("Starting Topology");
		TestStormTopologyBuilder testStormTopologyBuilder = new TestStormTopologyBuilder();
		    //Pass yaml file name which is present in your resources folder and id/name of your bolt defined in yaml file which you want to test
        testStormTopology = testStormTopologyBuilder.buildTopology("test-topology.yaml", "add-bolt");
  
        boolean topologyStatus = testStormTopology.startTopology(localCluster, 10, TimeUnit.SECONDS);
        //wait till topology is started, if it returns true that means topology has been started succesfully and ready to take tuples. i.e. open method of spout has been called
        // I have given max 10 seconds to start topology, it should start with in this time frame but if you need to increase time you can , also decreasing time wont affect performance so keep itas max as you want
        Assert.assertTrue(topologyStatus);
        logger.info("Topology Started");
	}
	@After
	public void stopTopology() throws Exception{
	//After every test kill this topology
		testStormTopology.killTopology(localCluster);
	}
	```
	
	
	Now Lets create our tests
	
	```java
  @Test
	public void test_WhenTwoValidPositiveIntegerValuesAreSentToBolt() throws Exception{
	      //sendMessageToStreamOfBolt is sync method, which means it will wait untill message has been processed by topology and will return after message is processed or given timeout has reached.
        boolean messageProcesssed = testStormTopology.sendMessageToStreamOfBolt("spout-1 --> bolt-1", "Test", new Values(5, 11), 5L, TimeUnit.SECONDS);
        //return value will i ftrue will tell message has been processed false means its timeout and message has not been prcessed fully
        Assert.assertTrue(messageProcesssed);
        List<Values> output = testStormTopology.getMessageReceivedOnStream("add-bolt --> print-bolt");
        Assert.assertEquals(1, output.size());
        Assert.assertEquals(16, output.get(0).get(0));
	}
	@Test
	public void test_WhenTwoValidOneNegativeAndOnePositivIntegerValuesAreSentToBolt() throws Exception{
        testStormTopology.sendMessageToStreamOfBolt("spout-1 --> bolt-1", "Test", new Values(-5, 11), 5L, TimeUnit.SECONDS);
        List<Values> output = testStormTopology.getMessageReceivedOnStream("add-bolt --> print-bolt");
        Assert.assertEquals(1, output.size());
        Assert.assertEquals(6, output.get(0).get(0));
	}
	
	@Test
	public void test_WhenMultiplePaorOfTwoValidPositiveIntegerValuesAreSentToBolt() throws Exception{
        testStormTopology.sendMessageToStreamOfBolt("spout-1 --> bolt-1", "Test01", new Values(5, 11), 5L, TimeUnit.SECONDS);
        testStormTopology.sendMessageToStreamOfBolt("spout-1 --> bolt-1", "Test02", new Values(-5, 11), 5L, TimeUnit.SECONDS);
        testStormTopology.sendMessageToStreamOfBolt("spout-1 --> bolt-1", "Test03", new Values(-105, 11), 5L, TimeUnit.SECONDS);
        testStormTopology.sendMessageToStreamOfBolt("spout-1 --> bolt-1", "Test04", new Values(13, 13), 5L, TimeUnit.SECONDS);
        testStormTopology.sendMessageToStreamOfBolt("spout-1 --> bolt-1", "Test05", new Values(0, 11), 5L, TimeUnit.SECONDS);


        List<Values> output = testStormTopology.getMessageReceivedOnStream("add-bolt --> print-bolt");
        Assert.assertEquals(5, output.size());
        Assert.assertEquals(16, output.get(0).get(0));
        Assert.assertEquals(6, output.get(1).get(0));
        Assert.assertEquals(-94, output.get(2).get(0));
        Assert.assertEquals(26, output.get(3).get(0));
        Assert.assertEquals(11, output.get(4).get(0));

	}
	@Test
	public void test_WhenTwoNonIntegerValuesAreSentToBolt() throws Exception{
        testStormTopology.sendMessageToStreamOfBolt("spout-1 --> bolt-1", "Test", new Values("Notinteger", "Hello"), 5L, TimeUnit.SECONDS);
        List<Values> output = testStormTopology.getMessageReceivedOnStream("add-bolt --> print-bolt");
        Assert.assertEquals(0, output.size());
	}
	```
	
	Thats it. :)
