package com.itextos.beacon.inmemory.templates;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.itextos.beacon.commonlib.utility.CommonUtility;
import com.itextos.beacon.errorlog.MemoryLoaderLog;
import com.itextos.beacon.inmemory.loader.process.AbstractAutoRefreshInMemoryProcessor;
import com.itextos.beacon.inmemory.loader.process.InmemoryInput;
import com.itextos.beacon.inmemory.templates.pojo.DLTMsgPrefixSuffixObj;

public class DLTMsgTemplatePrefixSuffix extends AbstractAutoRefreshInMemoryProcessor {
    // Optimized constants for minimal CPU overhead
    private static final int BATCH_SIZE = 500; // Increased significantly to reduce batching frequency
    private static final int LOG_FREQUENCY = 2000; // Reduced logging frequency
    private static final long MAX_PROCESS_TIME_MS = 15000; // Overall timeout
    private static final int MAX_RECORDS = 5000; // Reasonable upper limit for this data type
    
    private static final Log log = LogFactory.getLog(DLTMsgTemplatePrefixSuffix.class);

    // Thread-safe optimized storage
    private volatile Map<String, DLTMsgPrefixSuffixObj> mMsgTemplatePrefixSuffixMap = new ConcurrentHashMap<>(64);
    private final AtomicInteger loadCount = new AtomicInteger(0);

    public DLTMsgTemplatePrefixSuffix(InmemoryInput aInmemoryInputDetail) {
        super(aInmemoryInputDetail);
    }

    public DLTMsgPrefixSuffixObj getMsgPrefixSuffixVal(String aDltTemplateType) {
        if (aDltTemplateType == null) {
            return null;
        }
        // Direct concurrent map access - no synchronization needed
        return mMsgTemplatePrefixSuffixMap.get(aDltTemplateType.toLowerCase());
    }

    @Override
    protected void processResultSet(ResultSet aResultSet) throws SQLException {
        if (aResultSet == null) {
            return;
        }

        long startTime = System.nanoTime();
        int processedCount = 0;
        long lastProgressLog = System.currentTimeMillis();

        if (log.isDebugEnabled()) {
            log.debug("Starting resultset processing for " + getClass().getSimpleName());
        }

        // Pre-sized map for optimal performance
        final Map<String, DLTMsgPrefixSuffixObj> lDltMsgSuffixPrefixMap = 
            new HashMap<>(estimateInitialSize(aResultSet));

        while (aResultSet.next() && processedCount < MAX_RECORDS) {
            // Process row with minimal overhead
            if (processSingleRecord(aResultSet, lDltMsgSuffixPrefixMap)) {
                processedCount++;
            }

            // Ultra-light batching - only yield occasionally
            if (processedCount % BATCH_SIZE == 0) {
                applyMicroThrottle(processedCount);
                
                // Progress monitoring with minimal overhead
                if (System.currentTimeMillis() - lastProgressLog > 5000) {
                    logProgress(processedCount, startTime);
                    lastProgressLog = System.currentTimeMillis();
                }
            }

            // Safety timeout
            if ((System.nanoTime() - startTime) > MAX_PROCESS_TIME_MS * 1_000_000L) {
              //  log.warn("Processing timeout after " + processedCount + " records");
              //  break;
            }
        }

        // Atomic map swap
        if (!lDltMsgSuffixPrefixMap.isEmpty()) {
            mMsgTemplatePrefixSuffixMap = new ConcurrentHashMap<>(lDltMsgSuffixPrefixMap);
            if (log.isInfoEnabled()) {
                long durationMs = (System.nanoTime() - startTime) / 1_000_000;
                MemoryLoaderLog.log(this.getClass().getName() +" Loaded " + processedCount + " DLT template records in " + durationMs + "ms");
            }
        }
        
        loadCount.incrementAndGet();
        MemoryLoaderLog.log(getClass().getSimpleName() + " completed: " + processedCount + " records");
    }

    private int estimateInitialSize(ResultSet rs) throws SQLException {
        // Conservative estimation for template data (typically small)
        try {
            if (rs.getType() != ResultSet.TYPE_FORWARD_ONLY) {
                int currentPos = rs.getRow();
                rs.last();
                int size = rs.getRow();
                if (currentPos > 0) {
                    rs.absolute(currentPos);
                } else {
                    rs.beforeFirst();
                }
                return Math.min(size + 16, 1024); // Cap at 1024
            }
        } catch (SQLException e) {
            // Fall through to default
        }
        return 64; // Default small size for template data
    }

    private boolean processSingleRecord(ResultSet rs, Map<String, DLTMsgPrefixSuffixObj> targetMap) 
            throws SQLException {
        // Direct field access with minimal overhead
        String templateType = rs.getString("template_type");
        String msgPrefix = rs.getString("msg_prefix");
        String msgSuffix = rs.getString("msg_suffix");

        // Fast validation
        if (templateType == null || templateType.trim().isEmpty()) {
            return false;
        }

        // Optimized string processing
        templateType = fastLowerCase(templateType.trim());
        
        // Handle null prefix/suffix efficiently
        if (msgPrefix != null) msgPrefix = msgPrefix.trim();
        if (msgSuffix != null) msgSuffix = msgSuffix.trim();

        // Skip empty objects if business logic allows
        if ((msgPrefix == null || msgPrefix.isEmpty()) && 
            (msgSuffix == null || msgSuffix.isEmpty())) {
            return false;
        }

        // Create object and store
        DLTMsgPrefixSuffixObj obj = new DLTMsgPrefixSuffixObj(
            msgPrefix != null ? msgPrefix : "", 
            msgSuffix != null ? msgSuffix : ""
        );
        
        targetMap.put(templateType, obj);
        return true;
    }

    private String fastLowerCase(String str) {
        if (str == null) return "";
        
        // Fast path for common case - string might already be lowercase
        boolean needsConversion = false;
        int len = str.length();
        
        // Quick check if conversion is needed
        for (int i = 0; i < len; i++) {
            char c = str.charAt(i);
            if (c >= 'A' && c <= 'Z') {
                needsConversion = true;
                break;
            }
        }
        
        return needsConversion ? str.toLowerCase() : str;
    }

    private void applyMicroThrottle(int processedCount) {
        // Minimal throttling - only for very large datasets
        if (processedCount > 1000) {
            Thread.yield(); // Minimal CPU yield
            
            // Only sleep for extremely large processing
            if (processedCount > 3000 && processedCount % 1000 == 0) {
                try {
                    Thread.sleep(2); // Micro-sleep
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    // Preserve interrupt status
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private void logProgress(int processedCount, long startTime) {
            long durationMs = (System.nanoTime() - startTime) / 1_000_000;
            if (durationMs > 0) {
                double recordsPerSec = processedCount / (durationMs / 1000.0);
                MemoryLoaderLog.log(this.getClass().getName()+ "Progress: " + processedCount + " records, " + 
                         String.format("%.1f", recordsPerSec) + " records/sec");
            }
        
    }

   
}