package com.itextos.beacon.inmemory.userheader;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.itextos.beacon.errorlog.MemoryLoaderLog;
import com.itextos.beacon.inmemory.loader.process.AbstractAutoRefreshInMemoryProcessor;
import com.itextos.beacon.inmemory.loader.process.InmemoryInput;

public class IntlUserHeaderInfo extends AbstractAutoRefreshInMemoryProcessor {
    // Optimized constants for CPU reduction
    private static final int BATCH_SIZE = 2000; // Increased batch size
    private static final int YIELD_FREQUENCY = 1000; // Yield less frequently	
    private static final long MAX_PROCESSING_TIME_MS = 30000; // 30-second timeout
    private static final int LOG_FREQUENCY = 5000; // Reduced logging
    private static final int MAX_RECORDS = 100000; // Safety limit
    
    
    // Thread-safe optimized storage
    private volatile Map<String, Set<String>> mHeaderInfo = new ConcurrentHashMap<>();
    private final AtomicInteger totalRecords = new AtomicInteger(0);

    public IntlUserHeaderInfo(InmemoryInput aInmemoryInputDetail) {
        super(aInmemoryInputDetail);
    }

    public boolean isHeaderMatches(String aClientId, String aHeader) {
        if (aClientId == null || aHeader == null) {
            return false;
        }
        
        // Direct concurrent map access - no synchronization needed
        Set<String> headers = mHeaderInfo.get(aClientId);
        return headers != null && headers.contains(aHeader.toLowerCase());
    }

    @Override
    protected void processResultSet(ResultSet aResultSet) throws SQLException {
        long startTime = System.nanoTime();
        int processedCount = 0;
        long lastProgressCheck = System.currentTimeMillis();


        // Pre-sized map for optimal performance
        final Map<String, Set<String>> lClientHeaderInfo = createPreSizedMap(aResultSet);

        while (aResultSet.next() && processedCount < MAX_RECORDS) {
            // Safety timeout check
            if (System.currentTimeMillis() - lastProgressCheck > MAX_PROCESSING_TIME_MS) {
              //  log.warn("Processing timeout after " + processedCount + " records");
              //  break;
            }

            // Process single row efficiently
            if (processSingleRecord(aResultSet, lClientHeaderInfo)) {
                processedCount++;
            }

            // Efficient batching with minimal overhead
            if (processedCount % BATCH_SIZE == 0) {
                applyMicroThrottling(processedCount, startTime);
            }

            // Infrequent yielding
            if (processedCount % YIELD_FREQUENCY == 0) {
                Thread.yield();
                lastProgressCheck = System.currentTimeMillis();
            }
        }

        // Atomic map update
        if (!lClientHeaderInfo.isEmpty()) {
            mHeaderInfo = new ConcurrentHashMap<>(lClientHeaderInfo);
            
                long durationMs = (System.nanoTime() - startTime) / 1_000_000;
                MemoryLoaderLog.log(this.getClass().getName()+" : Loaded " + processedCount + " international header records in " + durationMs + "ms");
            
        }
        
        totalRecords.set(processedCount);
    }

    private Map<String, Set<String>> createPreSizedMap(ResultSet rs) throws SQLException {
        // Conservative size estimation
        int estimatedSize = 500; // Default for client-header data
        try {
            if (rs.getType() != ResultSet.TYPE_FORWARD_ONLY) {
                int currentRow = rs.getRow();
                rs.last();
                int rowCount = rs.getRow();
                if (rowCount > 0) {
                    // Estimate unique clients - typically fewer clients with multiple headers
                    estimatedSize = Math.min(rowCount / 10 + 100, 10000); // Cap at 10k clients
                }
                if (currentRow > 0) {
                    rs.absolute(currentRow);
                } else {
                    rs.beforeFirst();
                }
            }
        } catch (SQLException e) {
            // Use default size
        }
        return new HashMap<>(estimatedSize + 64); // Small buffer
    }

    private boolean processSingleRecord(ResultSet rs, Map<String, Set<String>> targetMap) throws SQLException {
        // Direct field access with minimal overhead
        String clientId = rs.getString("cli_id");
        String header = rs.getString("header");

        // Fast validation - skip invalid rows early
        if (clientId == null || header == null || 
            clientId.trim().isEmpty() || header.trim().isEmpty()) {
            return false;
        }

        // Optimized string processing
        clientId = fastTrim(clientId);
        header = fastTrim(header).toLowerCase();

        // Get or create header set for this client
        Set<String> headers = targetMap.get(clientId);
        if (headers == null) {
            headers = new HashSet<>(4); // Small initial size (most clients have few headers)
            targetMap.put(clientId, headers);
        }
        headers.add(header);
        
        return true;
    }

    private String fastTrim(String str) {
        if (str == null) return "";
        if (str.isEmpty()) return str;
        
        int len = str.length();
        int start = 0;
        int end = len;
        
        // Find first non-whitespace character
        while (start < end && str.charAt(start) <= ' ') {
            start++;
        }
        
        // Find last non-whitespace character  
        while (end > start && str.charAt(end - 1) <= ' ') {
            end--;
        }
        
        // Return original if no trimming needed
        if (start == 0 && end == len) {
            return str;
        }
        
        // Return empty if all whitespace
        if (start >= end) {
            return "";
        }
        
        return str.substring(start, end);
    }

    private void applyMicroThrottling(int processedCount, long startTime) {
        // Minimal throttling - only for large datasets
        if (processedCount > 5000) {
            Thread.yield();
            
            // Micro-sleep for very large datasets
            if (processedCount > 20000 && processedCount % 10000 == 0) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        
    
    }

    // Alternative ultra-fast processing method for maximum performance
    private boolean processSingleRecordUltraFast(ResultSet rs, Map<String, Set<String>> targetMap) 
            throws SQLException {
        try {
            // Ultra-fast path - assume data is clean
            String clientId = rs.getString("cli_id").trim();
            String header = rs.getString("header").trim().toLowerCase();
            
            Set<String> headers = targetMap.get(clientId);
            if (headers == null) {
                headers = new HashSet<>(2);
                targetMap.put(clientId, headers);
            }
            headers.add(header);
            
            return true;
        } catch (Exception e) {
            // Skip problematic rows
            return false;
        }
    }

    // Bulk optimization for better performance with large datasets
    private void processBatchOptimized(ResultSet rs, Map<String, Set<String>> targetMap, 
                                     int batchSize) throws SQLException {
        // Process multiple rows with reduced per-row overhead
        for (int i = 0; i < batchSize && rs.next(); i++) {
            processSingleRecord(rs, targetMap);
        }
    }

    // Monitoring and utility methods
    public int getTotalClients() {
        return mHeaderInfo.size();
    }
    
    public int getTotalHeaders() {
        return mHeaderInfo.values().stream()
                         .mapToInt(Set::size)
                         .sum();
    }
    
    public int getTotalRecordsLoaded() {
        return totalRecords.get();
    }
    
    public void clearCache() {
        mHeaderInfo.clear();
        totalRecords.set(0);
    }
    
    public Map<String, Integer> getClientHeaderCounts() {
        Map<String, Integer> counts = new HashMap<>();
        mHeaderInfo.forEach((clientId, headers) -> 
            counts.put(clientId, headers.size())
        );
        return counts;
    }
}