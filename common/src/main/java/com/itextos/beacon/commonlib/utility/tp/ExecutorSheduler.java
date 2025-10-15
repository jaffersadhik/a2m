package com.itextos.beacon.commonlib.utility.tp;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ExecutorSheduler {
	private static ExecutorSheduler obj=new ExecutorSheduler();
	
    private final ExecutorService virtualThreadPool;
    
    // Private constructor for singleton
    private ExecutorSheduler() {
        // Create a fixed pool of 5 virtual threads
        this.virtualThreadPool = Executors.newFixedThreadPool(16, Thread.ofVirtual().factory());
    }
    
    // Public method to get the singleton instance
    public static ExecutorSheduler getInstance() {
    	
    	if(obj==null) {
    		
    		obj=new ExecutorSheduler();
    	}
        return obj;
    }

    // Method to add tasks to the thread pool
    public void addTask(Runnable task, String threadName) {
        virtualThreadPool.submit(task);
    }
    
    // Optional: Method to shutdown the executor
    public void shutdown() {
        virtualThreadPool.shutdown();
    }
}