package com.itextos.beacon.platform.sbc.util;

import java.util.ArrayList;
import java.util.List;

public class ConsumerId {

    private static ConsumerId obj = new ConsumerId();
    
    final List<String> CONSUMER_ID_LIST = new ArrayList<String>();
    
    private int index = 0;
    
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
    
    public synchronized String getConsumerId() {
        if (CONSUMER_ID_LIST.isEmpty()) {
            throw new IllegalStateException("Consumer ID list is empty");
        }
        
        String result = CONSUMER_ID_LIST.get(index);
        index = (index + 1) % CONSUMER_ID_LIST.size();
        return result;
    }
    
    
    public List<String> getConsumerList(){
    	
    	return CONSUMER_ID_LIST;
    }
  
}