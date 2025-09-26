package com.itextos.beacon.inmemory.userheader;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.itextos.beacon.errorlog.MemoryLoaderLog;
import com.itextos.beacon.inmemory.loader.process.AbstractAutoRefreshInMemoryProcessor;
import com.itextos.beacon.inmemory.loader.process.InmemoryInput;

public class DomesticUserHeaderInfo extends AbstractAutoRefreshInMemoryProcessor {
    // Optimized constants for CPU reduction
    private static final int BATCH_SIZE = 2000; // Increased to reduce batching frequency
    private static final int YIELD_FREQUENCY = 500; // Yield every 500 records
    private static final long MAX_PROCESSING_TIME_MS = 30000; // 30-second timeout
    private static final int LOG_FREQUENCY = 5000; // Reduced logging
    

    // Thread-safe optimized storage
    private volatile Map<String, String> mTemplateGroupHeaderEntiryIdMap = new ConcurrentHashMap<>();
    private volatile Map<String, Set<String>> mTemplateGroupHeaderTemplateIdMap = new ConcurrentHashMap<>();
    private final AtomicInteger totalRecords = new AtomicInteger(0);

    public DomesticUserHeaderInfo(InmemoryInput aInmemoryInputDetail) {
        super(aInmemoryInputDetail);
    }

    public boolean isHeaderMatches(String aTemplateGroupId, String aHeader) {
        if (aTemplateGroupId == null || aHeader == null) {
            return false;
        }
        String key = buildKeyFast(aTemplateGroupId, aHeader);
        return mTemplateGroupHeaderEntiryIdMap.containsKey(key);
    }

    public String getEntityId(String aTemplateGroupId, String aHeader) {
        if (aTemplateGroupId == null || aHeader == null) {
            return null;
        }
        String key = buildKeyFast(aTemplateGroupId, aHeader);
        return mTemplateGroupHeaderEntiryIdMap.get(key);
    }

    public Set<String> getTemplateIds(String aTemplateGroupId, String aHeader) {
        if (aTemplateGroupId == null || aHeader == null) {
            return null;
        }
        String key = buildKeyFast(aTemplateGroupId, aHeader);
        return mTemplateGroupHeaderTemplateIdMap.get(key);
    }

    @Override
    protected void processResultSet(ResultSet aResultSet) throws SQLException {
        long startTime = System.nanoTime();
        int processedCount = 0;
        long lastYieldTime = System.currentTimeMillis();

      

        // Pre-sized maps for optimal performance
        final Map<String, String> lTemplateGroupHeaderEntiryIdMap = new HashMap<>(estimateSize(aResultSet));
        final Map<String, Set<String>> lTemplateGroupHeaderTemplateIdMap = new HashMap<>(estimateSize(aResultSet));

        while (aResultSet.next() && processedCount < Integer.MAX_VALUE) {
            // Safety timeout check
            if (System.currentTimeMillis() - lastYieldTime > MAX_PROCESSING_TIME_MS) {
             ////   log.warn("Processing timeout after " + processedCount + " records");
             //   break;
            }

            // Process single row efficiently
            if (processSingleRecord(aResultSet, lTemplateGroupHeaderEntiryIdMap, lTemplateGroupHeaderTemplateIdMap)) {
                processedCount++;
            }

            // Optimized batching with minimal overhead
            if (processedCount % BATCH_SIZE == 0) {
                applyEfficientThrottling(processedCount, startTime);
                lastYieldTime = System.currentTimeMillis();
            }

            // Periodic yielding for large datasets
            if (processedCount % YIELD_FREQUENCY == 0) {
                Thread.yield();
            }
        }

        // Atomic map updates
        if (!lTemplateGroupHeaderEntiryIdMap.isEmpty()) {
            mTemplateGroupHeaderEntiryIdMap = new ConcurrentHashMap<>(lTemplateGroupHeaderEntiryIdMap);
            mTemplateGroupHeaderTemplateIdMap = new ConcurrentHashMap<>(lTemplateGroupHeaderTemplateIdMap);
            
        
                long durationMs = (System.nanoTime() - startTime) / 1_000_000;
                MemoryLoaderLog.log(this.getClass().getName()+" Loaded " + processedCount + " header records in " + durationMs + "ms");
            
        }
        
        totalRecords.set(processedCount);
    }

    private int estimateSize(ResultSet rs) throws SQLException {
        // Conservative estimation
        int estimatedSize = 1000;
        try {
            if (rs.getType() != ResultSet.TYPE_FORWARD_ONLY) {
                int currentRow = rs.getRow();
                rs.last();
                int rowCount = rs.getRow();
                if (rowCount > 0) {
                    estimatedSize = Math.min(rowCount + 100, 50000); // Cap at 50k
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
        return estimatedSize;
    }

    private boolean processSingleRecord(ResultSet rs, 
                                      Map<String, String> entityIdMap,
                                      Map<String, Set<String>> templateIdMap) throws SQLException {
        // Direct field access with minimal overhead
        String templateGroupId = rs.getString("template_group_id");
        String header = rs.getString("header");
        String entityId = rs.getString("entity_id");
        String templateId = rs.getString("template_id");

        // Fast validation
        if (templateGroupId == null || header == null || 
            templateGroupId.trim().isEmpty() || header.trim().isEmpty()) {
            return false;
        }

        // Optimized string processing
        templateGroupId = fastTrim(templateGroupId);
        header = fastTrim(header).toLowerCase();
        entityId = entityId != null ? fastTrim(entityId) : "";
        templateId = templateId != null ? fastTrim(templateId) : "";

        // Skip if templateId is empty (if business logic allows)
        if (templateId.isEmpty()) {
            return false;
        }

        // Build key efficiently
        String key = buildKeyFast(templateGroupId, header);
        
        // Store entity ID
        entityIdMap.put(key, entityId);
        
        // Store template ID in set
        Set<String> templateSet = templateIdMap.get(key);
        if (templateSet == null) {
            templateSet = new HashSet<>(4); // Small initial size for template sets
            templateIdMap.put(key, templateSet);
        }
        templateSet.add(templateId);
        
        return true;
    }

    private String buildKeyFast(String templateGroupId, String header) {
        // Ultra-fast key generation for common cases
        int totalLength = templateGroupId.length() + header.length();
        if (totalLength < 32) {
            return templateGroupId + '|' + header;
        }
        
        // Use StringBuilder for longer keys
        StringBuilder sb = new StringBuilder(totalLength + 1);
        sb.append(templateGroupId).append('|').append(header);
        return sb.toString();
    }

    private String fastTrim(String str) {
        if (str == null) return "";
        
        int len = str.length();
        int start = 0;
        int end = len;
        
        // Find first non-whitespace
        while (start < end && str.charAt(start) <= ' ') {
            start++;
        }
        
        // Find last non-whitespace
        while (end > start && str.charAt(end - 1) <= ' ') {
            end--;
        }
        
        return (start > 0 || end < len) ? str.substring(start, end) : str;
    }

    private void applyEfficientThrottling(int processedCount, long startTime) {
        // Minimal throttling - only for very large datasets
        if (processedCount > 10000) {
            Thread.yield();
            
            // Micro-sleep for extremely large processing
            if (processedCount > 50000 && processedCount % 10000 == 0) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        
        // Progress logging (reduced frequency)
        if (processedCount % LOG_FREQUENCY == 0 ) {
            long durationMs = (System.nanoTime() - startTime) / 1_000_000;
            if (durationMs > 0) {
                double recordsPerSec = processedCount / (durationMs / 1000.0);
              
            }
        }
    }

   
    // Monitoring and utility methods
    public int getCacheSize() {
        return mTemplateGroupHeaderEntiryIdMap.size();
    }
    
    public int getTotalRecordsLoaded() {
        return totalRecords.get();
    }
    
    public void clearCache() {
        mTemplateGroupHeaderEntiryIdMap.clear();
        mTemplateGroupHeaderTemplateIdMap.clear();
        totalRecords.set(0);
    }
    
    public Map<String, String> getCacheSnapshot() {
        return new HashMap<>(mTemplateGroupHeaderEntiryIdMap);
    }
}