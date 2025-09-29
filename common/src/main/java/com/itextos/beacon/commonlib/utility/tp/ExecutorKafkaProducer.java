package com.itextos.beacon.commonlib.utility.tp;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ExecutorKafkaProducer {
    private final ExecutorService virtualThreadPool;
    
    // Private constructor for singleton
    private ExecutorKafkaProducer() {
        // Create a fixed pool of 5 virtual threads
        this.virtualThreadPool = Executors.newFixedThreadPool(2, Thread.ofVirtual().factory());
    }
    
    // Public method to get the singleton instance
    public static ExecutorKafkaProducer getInstance() {
        return new ExecutorKafkaProducer();
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