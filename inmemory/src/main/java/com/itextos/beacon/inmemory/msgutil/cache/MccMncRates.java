package com.itextos.beacon.inmemory.msgutil.cache;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.itextos.beacon.commonlib.utility.CommonUtility;
import com.itextos.beacon.errorlog.MemoryLoaderLog;
import com.itextos.beacon.inmemory.loader.process.AbstractAutoRefreshInMemoryProcessor;
import com.itextos.beacon.inmemory.loader.process.InmemoryInput;

public class MccMncRates extends AbstractAutoRefreshInMemoryProcessor {
    // Optimized constants for CPU reduction
    private static final int BATCH_SIZE = 200; // Increased to reduce batching overhead
    private static final long MAX_PROCESSING_TIME_MS = 10000; // Reduced timeout
    private static final long THROTTLE_DELAY_MS = 5; // Minimal delay
    private static final int MAX_RECORDS = 100000; // Safety limit
    private static final int LOG_FREQUENCY = 1000; // Reduced logging frequency
    
    private static final Log log = LogFactory.getLog(MccMncRates.class);

    // Optimized data storage
    private volatile Map<String, IntlSmsRates> mCustomerIntlCreditsMap = new HashMap<>();
    private final AtomicInteger processCount = new AtomicInteger(0);

    public MccMncRates(InmemoryInput aInmemoryInputDetail) {
        super(aInmemoryInputDetail);
    }

    public IntlSmsRates getCustomerCredits(String aClientId, String aCountry, String mcc, String mnc) {
        if (aClientId == null || aCountry == null || mcc == null || mnc == null) {
            return null;
        }
        
        // Optimized key generation without object creation
        String key = buildKey(aClientId, aCountry, mcc, mnc);
        return mCustomerIntlCreditsMap.get(key);
    }

    @Override
    protected void processResultSet(ResultSet aResultSet) throws SQLException {
        long startTime = System.nanoTime(); // More precise timing
        int count = 0;
        int totalProcessed = 0;

        	MemoryLoaderLog.log("Starting resultset processing for " + this.getClass().getSimpleName());
        

        // Pre-size map if possible
        final Map<String, IntlSmsRates> lCustomerCreditMap = createPreSizedMap(aResultSet);
        long lastLogTime = System.currentTimeMillis();

        while (aResultSet.next() && totalProcessed < MAX_RECORDS) {
            // Early exit if taking too long
            if (totalProcessed > 0 && totalProcessed % 1000 == 0) {
                if (System.currentTimeMillis() - lastLogTime > MAX_PROCESSING_TIME_MS) {
                 //   log.warn("Processing timeout after " + totalProcessed + " records");
                 //   break;
                }
            }

            // Process single row with minimal overhead
            if (processRow(aResultSet, lCustomerCreditMap)) {
                count++;
                totalProcessed++;
            }

            // Efficient batching with minimal overhead
            if (count >= BATCH_SIZE) {
                applyMinimalThrottling(count, totalProcessed);
                count = 0;
                
                // Infrequent progress logging
                if (totalProcessed % LOG_FREQUENCY == 0) {
                    logProgress(totalProcessed, startTime);
                    lastLogTime = System.currentTimeMillis();
                }
            }
        }

        // Atomic map replacement
        if (!lCustomerCreditMap.isEmpty()) {
            mCustomerIntlCreditsMap = lCustomerCreditMap;
                long durationMs = (System.nanoTime() - startTime) / 1_000_000;
                MemoryLoaderLog.log("Loaded " + totalProcessed + " rate records in " + durationMs + "ms");
            
        }
        
        processCount.incrementAndGet();
    }

    private Map<String, IntlSmsRates> createPreSizedMap(ResultSet rs) throws SQLException {
        int estimatedSize = 1000; // Conservative default
        try {
            if (rs.getType() != ResultSet.TYPE_FORWARD_ONLY) {
                int currentRow = rs.getRow();
                rs.last();
                int rowCount = rs.getRow();
                if (rowCount > 0) {
                    estimatedSize = Math.min(rowCount, 50000); // Cap estimation
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
        return new HashMap<>(estimatedSize + 256); // Small buffer
    }

    private boolean processRow(ResultSet rs, Map<String, IntlSmsRates> targetMap) throws SQLException {
        // Direct field access with minimal null checks
        String cliId = rs.getString("cli_id");
        String country = rs.getString("country");
        String mcc = rs.getString("mcc");
        String mnc = rs.getString("mnc");
        String baseSmsRate = rs.getString("base_sms_rate");
        String baseAddFixedRate = rs.getString("base_add_fixed_rate");

        // Fast validation - skip invalid rows early
        if (cliId == null || country == null || mcc == null || mnc == null || 
            baseSmsRate == null || baseAddFixedRate == null) {
            return false;
        }

        // Optimized string processing
        cliId = fastTrim(cliId);
        country = fastTrim(country);
        mcc = fastTrim(mcc);
        mnc = fastTrim(mnc);

        if (cliId.isEmpty() || country.isEmpty()) {
            return false;
        }

        // Efficient double parsing with error handling
        double smsRate = fastParseDouble(baseSmsRate);
        double addFixedRate = fastParseDouble(baseAddFixedRate);

        // Skip zero rates if that makes business sense
        if (smsRate == 0.0 && addFixedRate == 0.0) {
            return false;
        }

        // Build key efficiently
        String key = buildKeyFast(cliId, country, mcc, mnc);
        targetMap.put(key, new IntlSmsRates(smsRate, addFixedRate));
        
        return true;
    }

    private String buildKeyFast(String clientId, String country, String mcc, String mnc) {
        // Pre-calculate length to avoid resizing
        int length = clientId.length() + country.length() + mcc.length() + mnc.length() + 3;
        StringBuilder sb = new StringBuilder(length);
        
        sb.append(clientId).append('|')
          .append(country.toUpperCase()).append('|')
          .append(mcc).append('|')
          .append(mnc);
          
        return sb.toString();
    }

    private String buildKey(String clientId, String country, String mcc, String mnc) {
        // Even faster version for small strings
        if (clientId.length() + country.length() + mcc.length() + mnc.length() < 32) {
            return clientId + '|' + country.toUpperCase() + '|' + mcc + '|' + mnc;
        }
        return buildKeyFast(clientId, country, mcc, mnc);
    }

    private double fastParseDouble(String value) {
        if (value == null || value.isEmpty()) {
            return 0.0;
        }
        try {
            // Direct parsing for common cases
            return Double.parseDouble(value.trim());
        } catch (NumberFormatException e) {
            // Fallback to utility method for complex cases
            return CommonUtility.getDouble(value);
        }
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
        
        if (start == 0 && end == len) {
            return str; // No trimming needed
        }
        if (start >= end) {
            return ""; // All whitespace
        }
        
        return str.substring(start, end);
    }

    private void applyMinimalThrottling(int batchCount, int totalProcessed) {
        // Very lightweight throttling
        if (totalProcessed > 10000) {
            // Only throttle for large datasets
            Thread.yield(); // Minimal CPU yield
            
            // Very occasional sleep for very large datasets
            if (totalProcessed > 50000 && totalProcessed % 5000 == 0) {
                try {
                    Thread.sleep(1); // Minimal sleep
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private void logProgress(int totalProcessed, long startTime) {
        if (log.isInfoEnabled()) {
            long durationMs = (System.nanoTime() - startTime) / 1_000_000;
            double recordsPerSecond = totalProcessed / (durationMs / 1000.0);
            log.info("Processed " + totalProcessed + " records (" + 
                    String.format("%.1f", recordsPerSecond) + " records/sec)");
        }
    }

   
}