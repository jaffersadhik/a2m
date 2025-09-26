package com.itextos.beacon.inmemory.userheader;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.itextos.beacon.commonlib.utility.CommonUtility;
import com.itextos.beacon.commonlib.utility.ItextosClient;
import com.itextos.beacon.errorlog.MemoryLoaderLog;
import com.itextos.beacon.inmemory.loader.process.AbstractAutoRefreshInMemoryProcessor;
import com.itextos.beacon.inmemory.loader.process.InmemoryInput;

public class VerifiedSmsUserHeaderInfo extends AbstractAutoRefreshInMemoryProcessor {
    
    private static final int BATCH_SIZE = 5000; // Increased batch size for fewer iterations
    private static final int YIELD_FREQUENCY = 5; // Yield every 5 batches to reduce context switching
    
    // Thread-safe collections with better memory footprint
    private volatile Map<String, Set<String>> verifiedSmsHeaders = new ConcurrentHashMap<>();
    private final AtomicLong lastProcessTime = new AtomicLong(0);
    private final AtomicLong recordCounter = new AtomicLong(0);
    
    // Cache for client hierarchy to avoid repeated object creation
    private final Map<String, String[]> clientHierarchyCache = new ConcurrentHashMap<>();
    
    public VerifiedSmsUserHeaderInfo(InmemoryInput inmemoryInputDetail) {
        super(inmemoryInputDetail);
    }

    /**
     * Optimized verification with reduced CPU cycles
     */
    public boolean isVerifiedSMSUserHeader(String clientId, String header) {
        // Early validation with minimal operations
        if (clientId == null || header == null || header.isEmpty()) {
            return false;
        }

        String normalizedHeader = header.toLowerCase();
        
        // Get client hierarchy from cache or create once
        String[] clientHierarchy = clientHierarchyCache.computeIfAbsent(clientId, 
            k -> buildClientHierarchy(clientId));

        // Single pass check through all relevant client IDs
        for (String hierarchyClientId : clientHierarchy) {
            Set<String> headers = verifiedSmsHeaders.get(hierarchyClientId);
            if (headers != null && headers.contains(normalizedHeader)) {
                return true;
            }
        }
        
        return false;
    }

    /**
     * Build client hierarchy once and reuse
     */
    private String[] buildClientHierarchy(String clientId) {
        ItextosClient customer = new ItextosClient(clientId);
        return new String[] {
            customer.getClientId(),
            customer.getAdmin(),
            customer.getSuperAdmin()
        };
    }

    @Override
    protected void processResultSet(ResultSet resultSet) throws SQLException {
        long startTime = System.currentTimeMillis();
        long recordCount = 0;
        int batchCount = 0;

  

        Map<String, Set<String>> newVerifiedHeaders = new ConcurrentHashMap<>();
        
        try {
            while (resultSet.next()) {
                String clientId = CommonUtility.nullCheck(resultSet.getString("cli_id"), true);
                String headerId = CommonUtility.nullCheck(resultSet.getString("vsms_registered_header"), true);

                // Skip empty entries efficiently
                if (clientId.isEmpty() && headerId.isEmpty()) {
                    continue;
                }

                // Use computeIfAbsent with HashSet for O(1) lookups later
                newVerifiedHeaders
                    .computeIfAbsent(clientId, k -> new HashSet<>())
                    .add(headerId.toLowerCase());
                
                recordCount++;
                
                // Batch processing with optimized frequency
                if (++batchCount % BATCH_SIZE == 0) {
                    // Controlled yielding - less frequent to reduce context switching overhead
                    if ((batchCount / BATCH_SIZE) % YIELD_FREQUENCY == 0) {
                        Thread.yield();
                    }
                    
                    // Optional: Add small delay to prevent CPU monopolization
                    if (System.currentTimeMillis() - startTime > 100) { // 100ms threshold
                        try {
                            Thread.sleep(1); // Minimal sleep to allow other threads
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                        startTime = System.currentTimeMillis();
                    }
                }
            }
        } finally {
            // Atomic swap for thread-safe updates
            if (!newVerifiedHeaders.isEmpty()) {
                this.verifiedSmsHeaders = newVerifiedHeaders;
                // Clear cache when data changes
          //      clientHierarchyCache.clear();
            }
            
            this.recordCounter.set(recordCount);
            this.lastProcessTime.set(System.currentTimeMillis());
            
            	MemoryLoaderLog.log(this.getClass().getName()+ " : Processed " + recordCount + " records in " + 
                        (System.currentTimeMillis() - startTime) + "ms");
            
        }
    }

    /**
     * Memory optimization: clear cache periodically or on demand
     */
    public void clearCache() {
        clientHierarchyCache.clear();
    }

    /**
     * Get statistics for monitoring
     */
    public Map<String, Object> getStats() {
        return Map.of(
            "recordCount", recordCounter.get(),
            "clientCount", verifiedSmsHeaders.size(),
            "lastProcessTime", lastProcessTime.get()
        );
    }
}