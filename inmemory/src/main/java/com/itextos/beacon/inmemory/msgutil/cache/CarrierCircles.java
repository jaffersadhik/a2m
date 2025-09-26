package com.itextos.beacon.inmemory.msgutil.cache;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.itextos.beacon.commonlib.constants.ErrorMessage;
import com.itextos.beacon.errorlog.MemoryLoaderLog;
import com.itextos.beacon.inmemory.loader.process.AbstractAutoRefreshInMemoryProcessor;
import com.itextos.beacon.inmemory.loader.process.InmemoryInput;

public class CarrierCircles extends AbstractAutoRefreshInMemoryProcessor {
    // Optimized configuration constants
    private static final int BATCH_SIZE = 100;
    private static final int YIELD_FREQUENCY = 25;
    private static final long MAX_BATCH_TIME_MS = 2000;
    private static final long THROTTLE_DELAY_MS = 10;
    private static final int MAX_RECORDS_BEFORE_BREAK = 50000;
    private static final double CPU_THRESHOLD = 75.0;
    
    
    // Optimized data structures
    private volatile Map<String, Map<String, CarrierCircle>> mCarrierCircleMap = new ConcurrentHashMap<>(10000);
    private final AtomicInteger totalProcessed = new AtomicInteger(0);
    private volatile long lastCpuCheck = 0;
    private volatile double currentCpuUsage = 0;

    public CarrierCircles(InmemoryInput aInmemoryInputDetail) {
        super(aInmemoryInputDetail);
    }

    public CarrierCircle getCarrierCircle(String aMobileNumber) {
        return getCarrierCircle(aMobileNumber, false);
    }

    public CarrierCircle getCarrierCircle(String aMobileNumber, boolean aReturnDefault) {
        if (aMobileNumber == null || aMobileNumber.length() < 4) {
            return aReturnDefault ? CarrierCircle.DEFAULT_CARRIER_CIRCLE : null;
        }
        
        // Optimized prefix extraction
        final String prefix = extractPrefix(aMobileNumber);
        final Map<String, CarrierCircle> inner = mCarrierCircleMap.get(prefix);

        if (inner == null) {
            return aReturnDefault ? CarrierCircle.DEFAULT_CARRIER_CIRCLE : null;
        }

        // Direct get without null check since ConcurrentHashMap doesn't allow null values
        final CarrierCircle lCarrierCircle = inner.get(aMobileNumber);
        return lCarrierCircle != null ? lCarrierCircle : 
               (aReturnDefault ? CarrierCircle.DEFAULT_CARRIER_CIRCLE : null);
    }

    @Override
    protected void processResultSet(ResultSet aResultSet) throws SQLException {
        long overallStartTime = System.currentTimeMillis();
        long lastBatchTime = overallStartTime;
        int batchCount = 0;
        int totalCount = 0;

        	MemoryLoaderLog.log("Starting resultset processing for " + this.getClass().getSimpleName());
       

        // Pre-size the map if possible
        final Map<String, Map<String, CarrierCircle>> lTempMscCodes = createOptimizedMap(aResultSet);

        while (aResultSet.next() && totalCount < MAX_RECORDS_BEFORE_BREAK) {
            // Check for overall timeout
            if (System.currentTimeMillis() - overallStartTime > 30000) { // 30s total timeout
          //      log.warn("Overall processing timeout reached after " + totalCount + " records");
            //    break;
            }

            // Process the row
            processSingleRow(aResultSet, lTempMscCodes);
            
            batchCount++;
            totalCount++;

            // Smart batching with multiple throttling strategies
            if (shouldProcessBatch(batchCount, totalCount, lastBatchTime)) {
                processBatchThrottling(batchCount, totalCount, overallStartTime);
                batchCount = 0;
                lastBatchTime = System.currentTimeMillis();
            }
        }

        // Atomic swap of the map reference
        if (!lTempMscCodes.isEmpty()) {
            mCarrierCircleMap = new ConcurrentHashMap<>(lTempMscCodes);
            	MemoryLoaderLog.log("Loaded " + totalCount + " carrier circle records in " + 
                        (System.currentTimeMillis() - overallStartTime) + "ms");
           
        }
        
        totalProcessed.set(totalCount);
    }

    private Map<String, Map<String, CarrierCircle>> createOptimizedMap(ResultSet rs) throws SQLException {
        // Try to estimate size for optimal pre-sizing
        int estimatedSize = 1000; // Default estimate
        try {
            if (rs.getType() != ResultSet.TYPE_FORWARD_ONLY) {
                int currentRow = rs.getRow();
                rs.last();
                estimatedSize = Math.max(rs.getRow(), 1000);
                if (currentRow > 0) {
                    rs.absolute(currentRow);
                } else {
                    rs.beforeFirst();
                }
            }
        } catch (SQLException e) {
            // Ignore - use default size
        	MemoryLoaderLog.log("Could not estimate result set size, using default");
        }
        
        return new HashMap<>(estimatedSize + estimatedSize/4); // 25% extra capacity
    }

    private void processSingleRow(ResultSet rs, Map<String, Map<String, CarrierCircle>> targetMap) 
            throws SQLException {
        // Direct string access with null checks - avoid method call overhead
        String lPrefix = rs.getString("prefix");
        String lMsc = rs.getString("msc");
        String lCarrier = rs.getString("carrier");
        String lCircle = rs.getString("circle");

        // Skip invalid rows early
        if (lPrefix == null || lMsc == null || lCarrier == null || lCircle == null) {
            return;
        }

        // Trim strings only if necessary
        lPrefix = lPrefix.trim();
        lMsc = lMsc.trim();

        // Get or create inner map
        Map<String, CarrierCircle> innerMap = targetMap.get(lPrefix);
        if (innerMap == null) {
            innerMap = new HashMap<>(100); // Pre-size inner maps
            targetMap.put(lPrefix, innerMap);
        }

        // Create CarrierCircle object
        innerMap.put(lMsc, new CarrierCircle(lCarrier.trim(), lCircle.trim()));
    }

    private boolean shouldProcessBatch(int batchCount, int totalCount, long lastBatchTime) {
        // Always process at batch size
        if (batchCount >= BATCH_SIZE) {
            return true;
        }
        
        // Time-based throttling
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastBatchTime > MAX_BATCH_TIME_MS) {
            return true;
        }
        
        // Periodic yielding
        if (totalCount % YIELD_FREQUENCY == 0) {
            return true;
        }
        
        // CPU-based throttling (check less frequently to avoid overhead)
        if (currentTime - lastCpuCheck > 2000) { // Check every 2 seconds
            lastCpuCheck = currentTime;
            currentCpuUsage = estimateCpuUsage();
            if (currentCpuUsage > CPU_THRESHOLD) {
            	MemoryLoaderLog.log("High CPU usage detected: " + currentCpuUsage + "%, increasing throttling");
                return true;
            }
        }
        
        return false;
    }

    private void processBatchThrottling(int batchCount, int totalCount, long overallStartTime) {
        // Adaptive delay based on system conditions
        long delay = calculateAdaptiveDelay(batchCount, totalCount, overallStartTime);
        
        if (delay > 0) {
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // Preserve interruption status
                Thread.currentThread().interrupt();
                return;
            }
        }
        
        // Yield thread control
        Thread.yield();
        
        // Efficient logging - only log every 1000 records or every 5 seconds
        if (totalCount % 1000 == 0 || System.currentTimeMillis() - overallStartTime > 5000) {
            //	MemoryLoaderLog.log("Processed " + totalCount + " records, elapsed: " + 
             //            (System.currentTimeMillis() - overallStartTime) + "ms");
            
        }
    }

    private long calculateAdaptiveDelay(int batchCount, int totalCount, long overallStartTime) {
        long baseDelay = THROTTLE_DELAY_MS;
        
        // Increase delay if CPU is high
        if (currentCpuUsage > CPU_THRESHOLD) {
            baseDelay = (long) (baseDelay * (currentCpuUsage / CPU_THRESHOLD));
        }
        
        // Increase delay if processing is very fast (low complexity)
        long elapsed = System.currentTimeMillis() - overallStartTime;
        if (elapsed > 0) {
            double recordsPerSecond = totalCount / (elapsed / 1000.0);
            if (recordsPerSecond > 5000) { // Very high processing rate
                baseDelay += 5;
            }
        }
        
        // Progressive delay based on total processed count
        if (totalCount > 10000) {
            baseDelay += (totalCount / 10000) * 2; // Add 2ms per 10k records
        }
        
        // Cap maximum delay
        return Math.min(baseDelay, 100); // Max 100ms delay
    }

    private double estimateCpuUsage() {
        try {
            // Simple memory-based CPU estimation
            Runtime runtime = Runtime.getRuntime();
            long totalMemory = runtime.totalMemory();
            long freeMemory = runtime.freeMemory();
            double memoryUsage = (double) (totalMemory - freeMemory) / runtime.maxMemory();
            
            // If memory usage is high, assume CPU is also stressed
            if (memoryUsage > 0.8) {
                return 85.0;
            }
            
            // Simple approximation - in production consider using OperatingSystemMXBean
            return memoryUsage * 100;
            
        } catch (Exception e) {
        	MemoryLoaderLog.log("Error estimating CPU usage"+ ErrorMessage.getStackTraceAsString(e));
            return 50.0; // Conservative default
        }
    }

    // Optimized prefix extraction
    private String extractPrefix(String mobileNumber) {
        // Direct substring without method calls for common case
        return mobileNumber.length() >= 4 ? mobileNumber.substring(0, 4) : "";
    }

    // Monitoring methods
    public int getTotalRecordsLoaded() {
        return totalProcessed.get();
    }
    
    public int getCacheSize() {
        return mCarrierCircleMap.size();
    }
    
    public void clearCache() {
        mCarrierCircleMap.clear();
        totalProcessed.set(0);
    }
    
   
}