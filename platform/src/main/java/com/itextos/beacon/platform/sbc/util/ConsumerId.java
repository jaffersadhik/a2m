package com.itextos.beacon.platform.sbc.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.itextos.beacon.commonlib.utility.RoundRobin;

public class ConsumerId {

    private static ConsumerId obj = new ConsumerId();
    
    final List<String> CONSUMER_ID_LIST = new ArrayList<String>();
    
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
    	
        final int index = RoundRobin.getInstance().getCurrentIndex("schedule_blockout_index", CONSUMER_ID_LIST.size());

        return CONSUMER_ID_LIST.get(index);
    }
    
    
    public List<String> getConsumerList(){
    	
    	return CONSUMER_ID_LIST;
    }
  
}