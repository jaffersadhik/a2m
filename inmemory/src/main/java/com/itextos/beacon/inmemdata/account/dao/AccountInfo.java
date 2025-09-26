package com.itextos.beacon.inmemdata.account.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.itextos.beacon.commonlib.accountsync.AccountLoader;
import com.itextos.beacon.commonlib.commondbpool.DBDataSourceFactory;
import com.itextos.beacon.commonlib.commondbpool.DatabaseSchema;
import com.itextos.beacon.commonlib.commondbpool.JndiInfoHolder;
import com.itextos.beacon.commonlib.constants.ErrorMessage;
import com.itextos.beacon.commonlib.constants.exception.ItextosException;
import com.itextos.beacon.commonlib.pwdencryption.Encryptor;
import com.itextos.beacon.commonlib.utility.CommonUtility;
import com.itextos.beacon.errorlog.MemoryLoaderLog;
import com.itextos.beacon.inmemdata.account.UserInfo;
import com.itextos.beacon.inmemory.loader.process.AbstractAutoRefreshInMemoryProcessor;
import com.itextos.beacon.inmemory.loader.process.InmemoryInput;

public class AccountInfo extends AbstractAutoRefreshInMemoryProcessor {

    
    // Batch processing constants
    private static final int BATCH_SIZE = 1000;
    private static final int YIELD_FREQUENCY = 5;
    private static final int MAX_PROCESSING_TIME_MS = 30000;
    
    // Column indices
    private static final int COL_INDEX_CLIENT_ID = 1;
    private static final int COL_INDEX_USER_NAME = 2;
    private static final int COL_INDEX_API_PASSWORD = 4;
    private static final int COL_INDEX_SMPP_PASSWORD = 5;
    private static final int COL_INDEX_STATUS = 6;

    // Thread-safe collections with better concurrency
    private volatile Map<String, UserInfo> userPassMap = new ConcurrentHashMap<>(1024, 0.75f, 16);
    private volatile Map<String, UserInfo> accessKeyMap = new ConcurrentHashMap<>(1024, 0.75f, 16);
    private volatile Map<String, UserInfo> clientIdMap = new ConcurrentHashMap<>(1024, 0.75f, 16);
    
    // Performance monitoring
    private final LongAdder totalProcessed = new LongAdder();
    private volatile long lastLoadTime = 0;

    // Cache for service details to avoid repeated loading
    private static volatile Map<String, List<String>> serviceDetailsCache;
    private static volatile long serviceDetailsLastLoad = 0;
    private static final long SERVICE_DETAILS_CACHE_TTL = 300000; // 5 minutes

    public AccountInfo(InmemoryInput inmemoryInputDetail) {
        super(inmemoryInputDetail);
    }

    @Override
    protected void processResultSet(ResultSet resultSet) throws SQLException {
        if (resultSet == null) return;

        long startTime = System.nanoTime();
        int processedCount = 0;
        int batchCounter = 0;
        long lastYieldTime = startTime;

       

        // Pre-sized maps for optimal performance
        Map<String, UserInfo> tempUserPassMap = new HashMap<>(estimateInitialCapacity(resultSet));
        Map<String, UserInfo> tempAccessKeyMap = new HashMap<>(estimateInitialCapacity(resultSet));
        Map<String, UserInfo> tempClientIdMap = new HashMap<>(estimateInitialCapacity(resultSet));

        try {
            while (resultSet.next()) {
                if (processSingleRecord(resultSet, tempUserPassMap, tempAccessKeyMap, tempClientIdMap)) {
                    processedCount++;
                    batchCounter++;
                }

                // Batch processing with CPU optimization
                if (batchCounter >= BATCH_SIZE) {
                    handleBatchProcessing(startTime, lastYieldTime, processedCount);
                    batchCounter = 0;
                    lastYieldTime = System.nanoTime();
                }

                // Safety timeout
                if ((System.nanoTime() - startTime) > MAX_PROCESSING_TIME_MS * 1_000_000L) {
               //     log.warn("Processing timeout after " + processedCount + " records");
               //     break;
                }
            }
        } finally {
            // Atomic swap of maps
            updateMaps(tempUserPassMap, tempAccessKeyMap, tempClientIdMap, processedCount, startTime);
        }
    }

    private int estimateInitialCapacity(ResultSet rs) throws SQLException {
        // Conservative estimation to avoid resizing
        try {
            if (rs.getType() != ResultSet.TYPE_FORWARD_ONLY) {
                int currentRow = rs.getRow();
                rs.last();
                int size = rs.getRow();
                if (currentRow > 0) {
                    rs.absolute(currentRow);
                } else {
                    rs.beforeFirst();
                }
                return Math.max(size + 16, 64);
            }
        } catch (SQLException e) {
            // Fall through to default
        }
        return 256; // Reasonable default
    }

    private boolean processSingleRecord(ResultSet rs, 
                                      Map<String, UserInfo> userMap,
                                      Map<String, UserInfo> accessMap,
                                      Map<String, UserInfo> clientMap) throws SQLException {
        try {
            String clientId = rs.getString(COL_INDEX_CLIENT_ID);
            if (clientId == null || clientId.trim().isEmpty()) {
                return false;
            }

            // Parallel password decryption
            String apiPass = CommonUtility.nullCheck(rs.getString(COL_INDEX_API_PASSWORD), true);
            String smppPass = CommonUtility.nullCheck(rs.getString(COL_INDEX_SMPP_PASSWORD), true);
            
            String decryptedApiPass = decryptApiPasswordOptimized(apiPass, clientId);
            String decryptedSmppPass = decryptSmppPasswordOptimized(smppPass, clientId);

            String userName = CommonUtility.nullCheck(rs.getString(COL_INDEX_USER_NAME), true);
            if (userName.isEmpty()) {
                throw new ItextosException("Invalid username specified for client: " + clientId);
            }

            userName = userName.toLowerCase(); // Single lowercase operation

            int status = CommonUtility.getInteger(rs.getString(COL_INDEX_STATUS), -1);

            UserInfo userInfo = new UserInfo(clientId, userName, decryptedApiPass, decryptedSmppPass, status);

            // Batch map operations
            userMap.put(userName, userInfo);
            clientMap.put(clientId, userInfo);

            if (decryptedApiPass != null && !decryptedApiPass.isEmpty()) {
                accessMap.put(decryptedApiPass, userInfo);
            }

            return true;
        } catch (Exception e) {
            String clientId = "unknown";
            try {
                clientId = rs.getString(COL_INDEX_CLIENT_ID);
            } catch (SQLException ex) {
                // Ignore secondary error
            }
            MemoryLoaderLog.log("Exception processing user information for client: " + clientId+ " : "+ ErrorMessage.getStackTraceAsString(e));
            return false;
        }
    }

    private String decryptApiPasswordOptimized(String encryptedPass, String clientId) {
        if (encryptedPass == null || encryptedPass.isEmpty()) {
            return "";
        }
        
        try {
            return Encryptor.getApiDecryptedPassword(encryptedPass);
        } catch (Exception e) {
           
            return "";
        }
    }

    private String decryptSmppPasswordOptimized(String encryptedPass, String clientId) {
        if (encryptedPass == null || encryptedPass.isEmpty()) {
            return "";
        }
        
        try {
            return Encryptor.getSmppDecryptedPassword(encryptedPass);
        } catch (Exception e) {
           
            return "";
        }
    }

    private void handleBatchProcessing(long startTime, long lastYieldTime, int processedCount) {
        // Controlled yielding to reduce CPU contention
        if ((processedCount / BATCH_SIZE) % YIELD_FREQUENCY == 0) {
            Thread.yield();
        }
        
        // Micro-sleep for very large datasets to prevent CPU monopolization
        if (processedCount > 5000) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        
        
    }

    private void updateMaps(Map<String, UserInfo> userMap, 
                           Map<String, UserInfo> accessMap, 
                           Map<String, UserInfo> clientMap,
                           int processedCount, long startTime) {
        if (!userMap.isEmpty()) {
            this.userPassMap = new ConcurrentHashMap<>(userMap);
            this.accessKeyMap = new ConcurrentHashMap<>(accessMap);
            this.clientIdMap = new ConcurrentHashMap<>(clientMap);
            this.lastLoadTime = System.currentTimeMillis();
            totalProcessed.add(processedCount);
        }

            long durationMs = (System.nanoTime() - startTime) / 1_000_000;
            MemoryLoaderLog.log(this.getClass().getName()+ " : Loaded " + processedCount + " account records in " + durationMs + "ms. " +
                    "Total users: " + userMap.size() + ", Total access keys: " + accessMap.size());
        
    }

    // Optimized lookup methods
    public UserInfo getUserByUser(String username) {
        if (username == null) return null;
        return userPassMap.get(username.toLowerCase());
    }

    public UserInfo getUserByAccessKey(String accessKey) {
        return accessKeyMap.get(accessKey);
    }

    public UserInfo getUserByClientId(String clientId) {
        return clientIdMap.get(clientId);
    }

    // Optimized account info retrieval with caching
    public static Map<String, String> getAccountInfo(String clientId) {
        if (clientId == null || clientId.trim().isEmpty()) {
            return null;
        }

        final String sql = "select * from accounts_view where cli_id=?";
        
        try (Connection con = DBDataSourceFactory.getConnectionFromThin(
                JndiInfoHolder.getInstance().getJndiInfoUsingName(DatabaseSchema.ACCOUNTS.getKey()));
             PreparedStatement pstmt = con.prepareStatement(sql)) {
            
            pstmt.setLong(1, Long.parseLong(clientId));
            
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return processAccountResultSet(rs, clientId);
                }
            }
        } catch (Exception e) {
        }
        
        return null;
    }

    private static Map<String, String> processAccountResultSet(ResultSet rs, String clientId) 
            throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        int colCount = rsmd.getColumnCount();
        
        Map<String, String> results = new HashMap<>(colCount + 8); // Pre-size
        
        // Bulk column processing
        for (int i = 1; i <= colCount; i++) {
            results.put(rsmd.getColumnName(i), CommonUtility.nullCheck(rs.getString(i), true));
        }
        
        // Add service details with caching
        Map<String, List<String>> serviceDetails = getCachedServiceDetails();
        if (serviceDetails != null) {
            List<String> serviceList = serviceDetails.get(clientId);
            if (serviceList != null) {
                for (String service : serviceList) {
                    results.put(service, "1");
                }
            }
        }
        
        return results;
    }

    private static Map<String, List<String>> getCachedServiceDetails() {
        long currentTime = System.currentTimeMillis();
        if (serviceDetailsCache == null || 
            (currentTime - serviceDetailsLastLoad) > SERVICE_DETAILS_CACHE_TTL) {
            try {
                serviceDetailsCache = AccountLoader.getServiceInfo();
                serviceDetailsLastLoad = currentTime;
            } catch (Exception e) {
            }
        }
        return serviceDetailsCache;
    }

    // Performance monitoring methods
    public int getTotalUserCount() {
        return userPassMap.size();
    }
    
    public long getTotalProcessedRecords() {
        return totalProcessed.longValue();
    }
    
    public long getLastLoadTimestamp() {
        return lastLoadTime;
    }
    
    /**
     * Memory optimization - clear maps if needed (for reload scenarios)
     */
    public void clearCache() {
        userPassMap.clear();
        accessKeyMap.clear();
        clientIdMap.clear();
    }
}