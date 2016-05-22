package com.next.storm.integration;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class StormNotifier {
    private Map<String, CountDownLatch> latches = new HashMap<String, CountDownLatch>();
    private static final StormNotifier instance = new StormNotifier();
    private StormNotifier(){
    	
    }
    public static StormNotifier getInstance(){
    	return instance;
    }

    public void startNotificationFlow(String id){
        CountDownLatch latch = latches.get(id);
        if(latch != null && latch.getCount() > 0){
            throw new RuntimeException("latch value is not 0, Some other code must have left it to non zero. This may be some issue in the code");
        }
        latch = new CountDownLatch(1);
        latches.put(id, latch);
    }
    public boolean waitForNotification(String id, long timeOut, TimeUnit timeUnit) throws InterruptedException {
        CountDownLatch latch = latches.get(id);
        boolean result = latch.await(timeOut, timeUnit);
        if(!result){
            latch.countDown();
        }
        return result;
    }
    public void sendProcessedSignal(String id) {
        CountDownLatch latch = latches.get(id);
        latch.countDown();
    }

}
