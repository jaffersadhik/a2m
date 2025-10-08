package com.winnovature.utils.utils;

public class ExecutorSheduler {

	private ExecutorSheduler() {}
  
    // Method to add tasks to the list of tasks
    public static void addTask(Runnable task) {
    
       Thread.ofVirtual().start(task);
    }

    
}
