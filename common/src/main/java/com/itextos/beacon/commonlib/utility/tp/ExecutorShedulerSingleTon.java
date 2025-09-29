package com.itextos.beacon.commonlib.utility.tp;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ExecutorShedulerSingleTon {
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
        return new ExecutorShedulerSingleTon();
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