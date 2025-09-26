package com.itextos.beacon.inmemory.templates;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

import com.itextos.beacon.errorlog.MemoryLoaderLog;
import com.itextos.beacon.inmemory.loader.process.AbstractAutoRefreshInMemoryProcessor;
import com.itextos.beacon.inmemory.loader.process.InmemoryInput;
import com.itextos.beacon.inmemory.templates.pojo.DLTMsgPrefixSuffixObj;

public class DLTMsgTemplatePrefixSuffix extends AbstractAutoRefreshInMemoryProcessor {
    // Optimized constants for minimal CPU overhead
    private static final int BATCH_SIZE = 2000; // Increased for fewer iterations
    private static final int YIELD_FREQUENCY = 10; // Yield every 10 batches
    private static final int MAX_RECORDS = 10000; // Increased reasonable limit
    private static final int PROGRESS_LOG_INTERVAL_MS = 10000; // Less frequent logging
    

    // Optimized thread-safe storage
    private volatile Map<String, DLTMsgPrefixSuffixObj> templateMap = new ConcurrentHashMap<>(64, 0.75f, 1);
    private final LongAdder loadCount = new LongAdder(); // Better for high contention
    private volatile long lastLoadTimestamp = 0;

    // Cache for common template types to avoid map lookups
    private final Map<String, DLTMsgPrefixSuffixObj> hotTemplateCache = new ConcurrentHashMap<>(16);

    public DLTMsgTemplatePrefixSuffix(InmemoryInput inmemoryInputDetail) {
        super(inmemoryInputDetail);
    }

    /**
     * Ultra-fast lookup with hot path optimization
     */
    public DLTMsgPrefixSuffixObj getMsgPrefixSuffixVal(String templateType) {
        if (templateType == null || templateType.isEmpty()) {
            return null;
        }
        
        // Fast path: check hot cache first (no string transformation)
        DLTMsgPrefixSuffixObj cached = hotTemplateCache.get(templateType);
        if (cached != null) {
            return cached;
        }
        
        // Normal path: lowercase and check main map
        String key = fastToLowerCase(templateType);
        DLTMsgPrefixSuffixObj result = templateMap.get(key);
        
        // Cache frequently accessed templates (simple LRU-like behavior)
        if (result != null && hotTemplateCache.size() < 20) {
            hotTemplateCache.put(templateType, result);
        }
        
        return result;
    }

    @Override
    protected void processResultSet(ResultSet resultSet) throws SQLException {
        if (resultSet == null) {
            return;
        }

        long startTime = System.nanoTime();
        int processedCount = 0;
        int batchCounter = 0;
        long lastLogTime = startTime;

    
        // Pre-sized map with optimal initial capacity
        Map<String, DLTMsgPrefixSuffixObj> newTemplateMap = 
            createOptimizedMap(estimateInitialCapacity(resultSet));

        try {
            while (resultSet.next() && processedCount < MAX_RECORDS) {
                if (processRecordOptimized(resultSet, newTemplateMap)) {
                    processedCount++;
                    batchCounter++;
                }

                // Batch processing with reduced frequency
                if (batchCounter >= BATCH_SIZE) {
                    handleBatchProcessing(processedCount, startTime, lastLogTime);
                    batchCounter = 0;
                    lastLogTime = System.nanoTime();
                }
            }
        } finally {
            // Atomic update with minimal overhead
            updateTemplateMap(newTemplateMap, processedCount, startTime);
        }
    }

    private Map<String, DLTMsgPrefixSuffixObj> createOptimizedMap(int initialCapacity) {
        return new HashMap<>(initialCapacity, 0.75f);
    }

    private int estimateInitialCapacity(ResultSet rs) throws SQLException {
        // Conservative estimation to avoid resizing
        try {
            if (rs.getType() != ResultSet.TYPE_FORWARD_ONLY) {
                int currentRow = rs.getRow();
                rs.last();
                int size = Math.min(rs.getRow(), MAX_RECORDS);
                if (currentRow > 0) {
                    rs.absolute(currentRow);
                } else {
                    rs.beforeFirst();
                }
                return Math.max(size + 8, 32); // Add buffer, minimum 32
            }
        } catch (SQLException e) {
            // Use default capacity
        }
        return 64;
    }

    private boolean processRecordOptimized(ResultSet rs, Map<String, DLTMsgPrefixSuffixObj> targetMap) 
            throws SQLException {
        // Direct column access by index would be faster but requires knowing column positions
        String templateType = rs.getString("template_type");
        
        // Fast fail on invalid data
        if (templateType == null || templateType.isEmpty()) {
            return false;
        }

        String msgPrefix = rs.getString("msg_prefix");
        String msgSuffix = rs.getString("msg_suffix");

        // Optimized null handling and trimming
        msgPrefix = optimizedTrim(msgPrefix);
        msgSuffix = optimizedTrim(msgSuffix);

        // Skip empty entries if business logic allows
        if ((msgPrefix == null || msgPrefix.isEmpty()) && 
            (msgSuffix == null || msgSuffix.isEmpty())) {
            return false;
        }

        // Use string interning or caching for common values
        templateType = fastToLowerCase(templateType.trim());
        
        // Create object only if needed (avoid object creation if duplicate key overwrites)
        if (!targetMap.containsKey(templateType)) {
            DLTMsgPrefixSuffixObj obj = new DLTMsgPrefixSuffixObj(
                msgPrefix != null ? msgPrefix : "", 
                msgSuffix != null ? msgSuffix : ""
            );
            targetMap.put(templateType, obj);
        } else {
            // Update existing or skip based on business rules
            // Current implementation overwrites, which is fine
            DLTMsgPrefixSuffixObj obj = new DLTMsgPrefixSuffixObj(
                msgPrefix != null ? msgPrefix : "", 
                msgSuffix != null ? msgSuffix : ""
            );
            targetMap.put(templateType, obj);
        }
        
        return true;
    }

    private String optimizedTrim(String str) {
        if (str == null) return null;
        
        // Fast trim implementation
        int len = str.length();
        int start = 0;
        int end = len;
        
        while (start < end && str.charAt(start) <= ' ') {
            start++;
        }
        while (end > start && str.charAt(end - 1) <= ' ') {
            end--;
        }
        
        return (start > 0 || end < len) ? str.substring(start, end) : str;
    }

    private String fastToLowerCase(String str) {
        if (str == null) return "";
        
        // Check if conversion is needed (most strings may already be lowercase)
        int len = str.length();
        for (int i = 0; i < len; i++) {
            char c = str.charAt(i);
            if (c >= 'A' && c <= 'Z') {
                return str.toLowerCase();
            }
        }
        return str;
    }

    private void handleBatchProcessing(int processedCount, long startTime, long lastLogTime) {
        // Controlled yielding - less frequent to reduce context switching
        if ((processedCount / BATCH_SIZE) % YIELD_FREQUENCY == 0) {
            Thread.yield();
        }
        
        // Progress logging with reduced frequency
        long currentTime = System.nanoTime();
        if ((currentTime - lastLogTime) > PROGRESS_LOG_INTERVAL_MS * 1_000_000L) {
         //   logProgress(processedCount, startTime);
        }
        
        // Micro-sleep only for very large processing
        if (processedCount > 5000) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // Restore interrupt status
                Thread.currentThread().interrupt();
            }
        }
    }

    private void updateTemplateMap(Map<String, DLTMsgPrefixSuffixObj> newMap, 
                                 int processedCount, long startTime) {
        if (!newMap.isEmpty()) {
            // Create concurrent map with optimal concurrency level
            this.templateMap = new ConcurrentHashMap<>(newMap);
        //    this.hotTemplateCache.clear(); // Clear cache on update
            this.lastLoadTimestamp = System.currentTimeMillis();
            
         
                long durationMs = (System.nanoTime() - startTime) / 1_000_000;
                MemoryLoaderLog.log(this.getClass().getName() + 
                    " Loaded " + processedCount + " records in " + durationMs + "ms");
           
        }
        
        loadCount.increment();
    }

   

    /**
     * Performance monitoring methods
     */
    public int getLoadedRecordCount() {
        return loadCount.intValue();
    }
    
    public int getCurrentTemplateCount() {
        return templateMap.size();
    }
    
    public long getLastLoadTime() {
        return lastLoadTimestamp;
    }
    
    /**
     * Memory optimization - clear hot cache if needed
     */
    public void clearHotCache() {
        hotTemplateCache.clear();
    }
}