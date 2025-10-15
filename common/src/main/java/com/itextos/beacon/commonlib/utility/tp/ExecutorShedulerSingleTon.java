package com.itextos.beacon.commonlib.utility.tp;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ExecutorShedulerSingleTon {
	
	private static ExecutorShedulerSingleTon obj=new ExecutorShedulerSingleTon();
	
    private final ExecutorService singleVirtualThreadExecutor;
    
    // Private constructor for singleton
    private ExecutorShedulerSingleTon() {
        // Create a single virtual thread executor
        this.singleVirtualThreadExecutor = Executors.newSingleThreadExecutor(
            Thread.ofVirtual().factory()
        );
    }
    
    // Public method to get the singleton instance
    public static ExecutorShedulerSingleTon getInstance() {
    	
    	if(obj==null) {
    		
    		obj=new ExecutorShedulerSingleTon();
    	}
        return obj;
    }

    // Method to add tasks to the single virtual thread
    public void addTask(Runnable task, String threadName) {
        singleVirtualThreadExecutor.submit(task);
    }
    
    // Optional: Method to shutdown the executor
    public void shutdown() {
        singleVirtualThreadExecutor.shutdown();
    }
}