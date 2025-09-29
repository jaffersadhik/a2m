package com.itextos.beacon.commonlib.utility.tp;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ExecutorShedulePoller {
    private final ExecutorService virtualThreadPool;
    
    // Private constructor for singleton
    private ExecutorShedulePoller() {
        // Create a fixed pool of 5 virtual threads
        this.virtualThreadPool = Executors.newFixedThreadPool(5, Thread.ofVirtual().factory());
    }
    
    // Public method to get the singleton instance
    public static ExecutorShedulePoller getInstance() {
        return new ExecutorShedulePoller();
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