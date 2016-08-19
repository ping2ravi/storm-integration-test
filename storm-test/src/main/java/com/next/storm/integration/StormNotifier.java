package com.next.storm.integration;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class StormNotifier {
    private static final String TOPOLOGY_STARTED = "TOPOLOGY_STARTED";
    private Map<String, CountDownLatch> latches = new HashMap<String, CountDownLatch>();
    private static final StormNotifier instance = new StormNotifier();
    private AtomicInteger totalMessageProcessed = new AtomicInteger();
    private StormNotifier(){
    	
    }
    public void reset(){
        latches.clear();;
        totalMessageProcessed = new AtomicInteger();
    }
    public static StormNotifier getInstance(){
    	return instance;
    }

    private void startNotificationFlow(String id){
        CountDownLatch latch = latches.get(id);
        if(latch != null && latch.getCount() > 0){
            throw new RuntimeException("latch value is not 0, Some other code must have left it to non zero. This may be some issue in the code");
        }
        latch = new CountDownLatch(1);
        latches.put(id, latch);
    }
    private boolean waitForNotification(String id, long timeOut, TimeUnit timeUnit) throws InterruptedException {
        CountDownLatch latch = latches.get(id);
        boolean result = latch.await(timeOut, timeUnit);
        if(!result){
            latch.countDown();
        }
        return result;
    }
    private void sendProcessedSignal(String id) {
        CountDownLatch latch = latches.get(id);
        if(latch == null){
            startNotificationFlow(id);
            latch = latches.get(id);
        }
        latch.countDown();
    }
    public void startMessageNotificationFlow(String id){
        startNotificationFlow(id);
    }
    public boolean waitForMessageToBeProcessed(String id, long timeOut, TimeUnit timeUnit) throws InterruptedException {
        return waitForNotification(id, timeOut, timeUnit);
    }
    public void sendMessageProcessedSignal(String id) {
        sendProcessedSignal(id);
        totalMessageProcessed.incrementAndGet();
    }
    public void startNotificationFlowForStartingTopology(){
        startNotificationFlow(TOPOLOGY_STARTED);
    }
    public void sendTopologyStartedSignal() {
        sendProcessedSignal(TOPOLOGY_STARTED);
    }
    public boolean waitForTopologyToStart(long timeOut, TimeUnit timeUnit) throws InterruptedException {
        return waitForNotification(TOPOLOGY_STARTED, timeOut, timeUnit);
    }

    public boolean waitUntilNMessagesAreProcessed(int n, int maxSeconds) {
        int maxWaitTimeInMs = 50;
        int maxTry = maxSeconds * 1000/ maxWaitTimeInMs;
        int tries = 0;
        boolean returnValue;
        System.out.println("Waiting for "+n+" acks, currently there are "+ totalMessageProcessed.get());

        while(returnValue = totalMessageProcessed.get() < n){
            System.out.println("Waiting for "+n+" acks, currently there are "+ totalMessageProcessed.get());
            try {
                Thread.sleep(maxWaitTimeInMs);
                tries++;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if(tries >= maxTry){
                break;
            }

        }
        System.out.println("Waiting for "+n+" acks, currently there are "+ totalMessageProcessed.get());

        return !returnValue;

    }

}
