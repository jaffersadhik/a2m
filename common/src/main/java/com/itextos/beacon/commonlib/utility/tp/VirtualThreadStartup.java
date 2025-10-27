package com.itextos.beacon.commonlib.utility.tp;

public interface VirtualThreadStartup {
	
	
    
   

    // Method to add tasks to the thread pool
    public static void addTask(Runnable task, String threadName) {
    	Thread.ofVirtual()
        .name(threadName)
        .start(task);
    }
    
   
}