package com.itextos.beacon.inmemory.inmemdata.mccmnc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.itextos.beacon.errorlog.MemoryLoaderLog;
import com.itextos.beacon.inmemory.loader.process.AbstractAutoRefreshInMemoryProcessor;
import com.itextos.beacon.inmemory.loader.process.InmemoryInput;

public class MccMncCollection
        extends
        AbstractAutoRefreshInMemoryProcessor
{
    // Configuration constants - can be externalized to properties
    private static final int BATCH_SIZE = 100;
    private static final int YIELD_FREQUENCY = 50;
    private static final long MAX_BATCH_TIME_MS = 2000; // Reduced from 5s to 2s
    private static final long THROTTLE_DELAY_MS = 50;   // Small delay between batches
    private static final double CPU_THRESHOLD = 80.0;
    private static final int MAX_ROWS_BEFORE_YIELD = 1000;
    
    private  Map<String, MccMncInfo> mMccMncData = new HashMap<>();
    private final AtomicInteger totalProcessed = new AtomicInteger(0);
    private volatile long lastCpuCheckTime = 0;
    private volatile double lastCpuUsage = 0;

    public MccMncCollection(
            InmemoryInput aInmemoryInputDetail)
    {
        super(aInmemoryInputDetail);
    }

    @Override
    protected void processResultSet(
            ResultSet aResultSet)
            throws SQLException
    {
        long startTime = System.currentTimeMillis();
        long lastYieldTime = startTime;
        int count = 0;
        int totalCount = 0;

        // Pre-size map if we know approximate size (optional optimization)
        if (aResultSet.getType() != ResultSet.TYPE_FORWARD_ONLY) {
            aResultSet.last();
            int estimatedSize = aResultSet.getRow();
            if (estimatedSize > 0) {
            //    mMccMncData.clear();
           //     mMccMncData = new HashMap<>(estimatedSize + estimatedSize/4); // 25% extra capacity
                aResultSet.beforeFirst();
            }
        }

        while (aResultSet.next())
        {
            final MccMncInfo ci = getMccMncInfoFromDB(aResultSet);
            
            // Use putIfAbsent for thread safety if needed, otherwise direct put
            mMccMncData.put(ci.getPrefix(), ci);
            
            count++;
            totalCount++;

            // Optimized batch processing with multiple throttling strategies
            if (count >= BATCH_SIZE || shouldThrottle(startTime, lastYieldTime, totalCount)) {
                processBatchThrottling(count, totalCount, startTime);
                count = 0;
                lastYieldTime = System.currentTimeMillis();
            }
        }

        // Log final statistics
        MemoryLoaderLog.log(this.getClass().getName() + " - Completed processing " + totalCount + " records in " + 
                           (System.currentTimeMillis() - startTime) + "ms");
        totalProcessed.set(totalCount);
    }

    private boolean shouldThrottle(long startTime, long lastYieldTime, int totalCount) {
        long currentTime = System.currentTimeMillis();
        
        // Time-based throttling
        if (currentTime - lastYieldTime > MAX_BATCH_TIME_MS) {
            return true;
        }
        
        // Count-based throttling
        if (totalCount % YIELD_FREQUENCY == 0) {
            return true;
        }
        
        // CPU-based throttling (check less frequently to avoid overhead)
        if (currentTime - lastCpuCheckTime > 1000) { // Check CPU every second
            lastCpuCheckTime = currentTime;
            lastCpuUsage = getSystemCpuUsage();
            if (lastCpuUsage > CPU_THRESHOLD) {
                MemoryLoaderLog.log("High CPU detected: " + lastCpuUsage + "%, throttling processing");
                return true;
            }
        }
        
        return false;
    }

    private void processBatchThrottling(int batchCount, int totalCount, long startTime) {
        // Adaptive delay based on system load
        long delay = calculateAdaptiveDelay(batchCount, totalCount, startTime);
        
        if (delay > 0) {
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // Restore interrupted status and exit gracefully
                Thread.currentThread().interrupt();
                return;
            }
        }
        
        // Yield thread control
        Thread.yield();
        
        // Log progress periodically (reduced frequency to save CPU)
        if (totalCount % 500 == 0) {
            MemoryLoaderLog.log(this.getClass().getName() + " - Processed " + totalCount + " records");
        }
        
        // Check for timeout conditions
        if (System.currentTimeMillis() - startTime > 30000) { // 30 second overall timeout
            MemoryLoaderLog.log("Processing taking too long, consider optimizing query or reducing load");
        }
    }

    private long calculateAdaptiveDelay(int batchCount, int totalCount, long startTime) {
        long baseDelay = THROTTLE_DELAY_MS;
        
        // Increase delay if CPU is high
        if (lastCpuUsage > CPU_THRESHOLD) {
            baseDelay = (long) (baseDelay * (lastCpuUsage / CPU_THRESHOLD));
        }
        
        // Increase delay if processing too fast (indicating low complexity)
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (elapsedTime > 0) {
            double rowsPerSecond = totalCount / (elapsedTime / 1000.0);
            if (rowsPerSecond > 10000) { // Very high processing rate
                baseDelay += 10; // Small additional delay
            }
        }
        
        // Cap maximum delay to prevent excessive slowing
        return Math.min(baseDelay, 500); // Max 500ms delay
    }

    private double getSystemCpuUsage() {
        try {
            // Simple CPU monitoring - you might want to use OSHI library for better accuracy
            Runtime runtime = Runtime.getRuntime();
            long totalMemory = runtime.totalMemory();
            long freeMemory = runtime.freeMemory();
            double memoryUsage = (double) (totalMemory - freeMemory) / runtime.maxMemory();
            
            // If memory usage is high, we might also want to throttle
            if (memoryUsage > 0.8) {
                return 90.0; // Simulate high CPU when memory is constrained
            }
            
            // For simple implementation, return a conservative estimate
            // In production, consider using OperatingSystemMXBean or OSHI library
            return memoryUsage * 100; // Rough approximation
            
        } catch (Exception e) {
            // If we can't determine CPU usage, be conservative
            return 50.0; // Assume moderate load
        }
    }

    // Optimized version of getMccMncInfoFromDB
    private static MccMncInfo getMccMncInfoFromDB(ResultSet aResultSet) throws SQLException {
        // Reduced method calls by inlining null checks
        String mcc = getSafeString(aResultSet.getString("mcc"));
        String mnc = getSafeString(aResultSet.getString("mnc"));
        String prefix = getSafeString(aResultSet.getString("prefix"));
        
        return new MccMncInfo(mcc, mnc, prefix);
    }
    
    // Optimized string handling to reduce method call overhead
    private static String getSafeString(String value) {
        if (value == null) {
            return "";
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? "" : trimmed;
    }

    public MccMncInfo getMccMncData(String prefix) {
        // Optional: add null check to avoid NullPointerException
        if (prefix == null) {
            return null;
        }
        return mMccMncData.get(prefix);
    }
    
    // Additional utility methods for monitoring
    public int getTotalProcessedCount() {
        return totalProcessed.get();
    }
    
    public int getCurrentCacheSize() {
        return mMccMncData.size();
    }
    
    public void clearCache() {
        mMccMncData.clear();
        totalProcessed.set(0);
    }
}