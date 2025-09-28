package com.itextos.beacon.inmemory.loader.process;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.itextos.beacon.commonlib.commondbpool.DBDataSourceFactory;
import com.itextos.beacon.inmemory.inmemdata.mccmnc.MccMncCollection;

public abstract class InmemoryProcessor
        implements
        IInmemoryProcess
{

    private static final Log log         = LogFactory.getLog(InmemoryProcessor.class);
    
   public static Map<String,Map<String,String>> TABLESUMMARY =new HashMap<String,Map<String,String>>();

    protected InmemoryInput  mInmemoryInput;
    private boolean          isFirstTime = true;
    
    // Pagination constants - can be made configurable
    private static final int DEFAULT_PAGE_SIZE = 1000;
    private static final int DEFAULT_FETCH_SIZE = 1000;

    protected InmemoryProcessor(
            InmemoryInput aInmemoryInputDetail)
    {
        mInmemoryInput = aInmemoryInputDetail;
    }

    @Override
    public void getDataFromDB()
    {
    	long start=System.currentTimeMillis();
    	
    	Map<String,String> data=TABLESUMMARY.get(mInmemoryInput.getInmemoryProcessClassName());
    	
    	if(data==null) {
    		
    		data=new HashMap<String,String>();
    		
    		TABLESUMMARY.put(mInmemoryInput.getInmemoryProcessClassName(), data);
    		    		
    	}
    	
    	data.put("recordcount", ""+getCount(getFirstTableName(mInmemoryInput.getSQL())));
    	data.put("tablename", getFirstTableName(mInmemoryInput.getSQL()));
    	
        if(this instanceof MccMncCollection) {
            doWithPagination();
        } else {
            doWithoutPagination();
        }
        
    	long end=System.currentTimeMillis();
    	
    	data.put("timetakenforfetch", ""+(end-start)+" in ms");


    }

    private int getCount(String tablename) {
    	
    	
    	 try (
                 Connection con = DBDataSourceFactory.getConnection(mInmemoryInput.getJNDIInfo());
                 PreparedStatement pstmt = con.prepareStatement("select count(*) cnt from "+tablename);
                 ResultSet mResultSet = pstmt.executeQuery();)
         {
            
    		 if(mResultSet.next()) {
    			 
    			 return mResultSet.getInt("cnt");
    		 }
         }
         catch (final Exception e)
         {
             log.error("ignorable Exception. Exception while doing inmemory load of '" + mInmemoryInput.getInmemoryId() + "'", e);

             if (isFirstTime)
             {
                 log.error("Since the initial load has failed, stopping the application for " + mInmemoryInput, e);
                 System.exit(-9);
             }
         }
    	 
    	 return 0;
    }
    private void doWithoutPagination() {
        try (
                Connection con = DBDataSourceFactory.getConnection(mInmemoryInput.getJNDIInfo());
                PreparedStatement pstmt = con.prepareStatement(mInmemoryInput.getSQL(), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                ResultSet mResultSet = pstmt.executeQuery();)
        {
            pstmt.setFetchSize(1000);
            processResultSet(mResultSet);
            isFirstTime = false;
        }
        catch (final Exception e)
        {
            log.error("ignorable Exception. Exception while doing inmemory load of '" + mInmemoryInput.getInmemoryId() + "'", e);

            if (isFirstTime)
            {
                log.error("Since the initial load has failed, stopping the application for " + mInmemoryInput, e);
                System.exit(-9);
            }
        }
    }

    protected void doWithPagination() {
        int pageNumber = 0;
        boolean hasMoreData = true;
        
        while (hasMoreData) {
            // ✅ NEW CONNECTION for EACH page
            hasMoreData = processPageWithNewConnection(pageNumber);
            pageNumber++;
            try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            log.debug("Processed page " + pageNumber + ", hasMoreData: " + hasMoreData);
        }
        
        isFirstTime = false;
        log.info("Completed processing all pages. Total pages: " + pageNumber);
    }

    /**
     * Process a page using a NEW connection for each page
     */
    private boolean processPageWithNewConnection(int pageNumber) {
        int pageSize = getPageSize();
        int offset = pageNumber * pageSize;
        
        String paginatedSQL = addPaginationToSQL(mInmemoryInput.getSQL(), offset, pageSize);
        
        // ✅ NEW CONNECTION for each page
        try (Connection connection = DBDataSourceFactory.getConnection(mInmemoryInput.getJNDIInfo());
             PreparedStatement pstmt = connection.prepareStatement(paginatedSQL, 
                     ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
             ResultSet resultSet = pstmt.executeQuery()) {
            
            pstmt.setFetchSize(getFetchSize());
            
            int rowCount = 0;
            boolean hasData = false;
            
            // Process results and count rows
            while (resultSet.next()) {
                processSingleRow(resultSet);
                rowCount++;
                hasData = true;
            }
            
            // If we got a full page, there might be more data
            // If we got less than a full page, this is the last page
            return rowCount == pageSize;
            
        } catch (final SQLException e) {
            log.error("Error processing page " + pageNumber + " for inmemory load of '" + 
                     mInmemoryInput.getInmemoryId() + "'", e);
            
            if (isFirstTime && pageNumber == 0) {
                log.error("Initial load failed, stopping application", e);
                System.exit(-9);
            }
            throw new RuntimeException("Failed to process page " + pageNumber, e);
        } catch (final Exception e) {
            log.error("General error processing page " + pageNumber + " for inmemory load of '" + 
                     mInmemoryInput.getInmemoryId() + "'", e);
            
            if (isFirstTime && pageNumber == 0) {
                log.error("Initial load failed, stopping application", e);
                System.exit(-9);
            }
            throw new RuntimeException("Failed to process page " + pageNumber, e);
        }
    }

    /**
     * Alternative: Process page with retry mechanism for each connection
     */
    private boolean processPageWithNewConnectionAndRetry(int pageNumber) {
        int maxRetries = 3;
        int retryCount = 0;
        
        while (retryCount <= maxRetries) {
            try {
                return processSinglePage(pageNumber);
            } catch (Exception e) {
                retryCount++;
                if (retryCount > maxRetries) {
                    log.error("Failed to process page " + pageNumber + " after " + maxRetries + " retries", e);
                    throw new RuntimeException("Failed to process page " + pageNumber + " after retries", e);
                }
                log.warn("Retry " + retryCount + " for page " + pageNumber + " due to: " + e.getMessage());
                
                // Wait before retry (exponential backoff)
                try {
                    Thread.sleep(1000 * retryCount);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Thread interrupted during retry", ie);
                }
            }
        }
        return false;
    }
    
    /**
     * Actual page processing logic
     * @throws Exception 
     */
    private boolean processSinglePage(int pageNumber) throws Exception {
        int pageSize = getPageSize();
        int offset = pageNumber * pageSize;
        
        String paginatedSQL = addPaginationToSQL(mInmemoryInput.getSQL(), offset, pageSize);
        
        // New connection for this page
        try (Connection connection = DBDataSourceFactory.getConnection(mInmemoryInput.getJNDIInfo());
             PreparedStatement pstmt = connection.prepareStatement(paginatedSQL, 
                     ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
             ResultSet resultSet = pstmt.executeQuery()) {
            
            pstmt.setFetchSize(getFetchSize());
            
            int rowCount = 0;
            boolean hasData = false;
            
            // Process results and count rows
            while (resultSet.next()) {
                processSingleRow(resultSet);
                rowCount++;
                hasData = true;
            }
            
            log.debug("Page " + pageNumber + " processed " + rowCount + " rows");
            return rowCount == pageSize;
        }
    }
    
    /**
     * Adds pagination to the SQL query based on database type
     */
    protected String addPaginationToSQL(String originalSQL, int offset, int pageSize) {
        if (originalSQL == null || originalSQL.trim().isEmpty()) {
            return originalSQL;
        }
        
        String sqlUpper = originalSQL.toUpperCase().trim();
        
        // Check if SQL already has pagination
        if (sqlUpper.contains("LIMIT") || sqlUpper.contains("OFFSET") || 
            sqlUpper.contains("ROWNUM") || sqlUpper.contains("FETCH")) {
            log.warn("SQL may already contain pagination clauses: " + originalSQL);
            return originalSQL;
        }
        
        // Simple pagination - adjust based on your database
        StringBuilder paginatedSQL = new StringBuilder(originalSQL);
        
        // Remove trailing semicolon if present
        if (originalSQL.endsWith(";")) {
            paginatedSQL = new StringBuilder(originalSQL.substring(0, originalSQL.length() - 1));
        }
        
        // Add pagination - using LIMIT/OFFSET syntax
        paginatedSQL.append(" LIMIT ").append(pageSize).append(" OFFSET ").append(offset);
        
        log.debug("Paginated SQL for page: " + paginatedSQL.toString());
        return paginatedSQL.toString();
    }

    /**
     * Process a single row from the result set
     */
    protected void processSingleRow(ResultSet resultSet) throws SQLException {
        // Default implementation - subclasses can override for row-by-row processing
        // For backward compatibility, call the abstract processResultSet method
        // This will need to be handled differently - see note below
    }

    /**
     * Process the entire result set (maintains backward compatibility)
     * NOTE: This needs adjustment for per-page processing
     */
    protected abstract void processResultSet(ResultSet mResultSet) throws SQLException;

    /**
     * Get the page size for pagination
     */
    protected int getPageSize() {
        String pageSizeStr = System.getenv("INMEMORY_PAGE_SIZE");
        if (pageSizeStr != null) {
            try {
                return Integer.parseInt(pageSizeStr);
            } catch (NumberFormatException e) {
                log.warn("Invalid INMEMORY_PAGE_SIZE value: " + pageSizeStr + ", using default: " + DEFAULT_PAGE_SIZE);
            }
        }
        return DEFAULT_PAGE_SIZE;
    }
    
    /**
     * Get the fetch size for JDBC
     */
    protected int getFetchSize() {
        String fetchSizeStr = System.getenv("INMEMORY_FETCH_SIZE");
        if (fetchSizeStr != null) {
            try {
                return Integer.parseInt(fetchSizeStr);
            } catch (NumberFormatException e) {
                log.warn("Invalid INMEMORY_FETCH_SIZE value: " + fetchSizeStr + ", using default: " + DEFAULT_FETCH_SIZE);
            }
        }
        return DEFAULT_FETCH_SIZE;
    }

    @Override
    public void getDataFromEJBServer() {
        // TODO: Implement pagination for EJB server calls if needed
    }
    
    @Override
    public void refreshInmemoryData() {
        String module = System.getenv("module");
        
        if(module != null && module.equals("dbgwejb")) {
            getDataFromDB();
        } else {
            String dbgw = System.getenv("dbgw");
            if(dbgw != null && dbgw.equals("ejb")) {
                getDataFromEJBServer();
            } else {
                getDataFromDB();
            }
        }
    
    }
    
    public static String getFirstTableName(String sql) {
        if (sql == null) return null;
        
        // Simple regex to find table after FROM
        Pattern pattern = Pattern.compile(
            "FROM\\s+(\\w+)", 
            Pattern.CASE_INSENSITIVE
        );
        
        Matcher matcher = pattern.matcher(sql);
        if (matcher.find()) {
            return matcher.group(1);
        }
        
        return null;
    }
}