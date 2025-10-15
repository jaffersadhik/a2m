package com.itextos.beacon.commonlib.utility.tp;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ExecutorSheduler2 {
	
	private static ExecutorSheduler2 obj=new ExecutorSheduler2();
	
    private final ExecutorService virtualThreadPool;
    
    // Private constructor for singleton
    private ExecutorSheduler2() {
        // Create a fixed pool of 5 virtual threads
        this.virtualThreadPool = Executors.newFixedThreadPool(16, Thread.ofVirtual().factory());
    }
    
    // Public method to get the singleton instance
    public static ExecutorSheduler2 getInstance() {
    	
    	if(obj==null) {
    		obj=new ExecutorSheduler2();
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