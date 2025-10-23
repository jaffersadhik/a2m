package com.itextos.beacon.platform.sbc.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class ConsumerId {

    private static ConsumerId obj = new ConsumerId();
    
    final List<String> CONSUMER_ID_LIST = new ArrayList<String>();
    
    private final AtomicInteger index = new AtomicInteger(0);
    
    private void init() {
        for(int i = 1; i < 6; i++) {
            CONSUMER_ID_LIST.add(i + "");
        }
    }
    
    private ConsumerId() {

    	init();
    }
    
    public static ConsumerId getInstance() {
        return obj;
    }
    
    public String getConsumerId() {
        if (CONSUMER_ID_LIST.isEmpty()) {
            throw new IllegalStateException("Consumer ID list is empty");
        }
        
        int currentIndex = index.getAndUpdate(i -> (i + 1) % CONSUMER_ID_LIST.size());
        return CONSUMER_ID_LIST.get(currentIndex);
    }
    
    
    public List<String> getConsumerList(){
    	
    	return CONSUMER_ID_LIST;
    }
  
}