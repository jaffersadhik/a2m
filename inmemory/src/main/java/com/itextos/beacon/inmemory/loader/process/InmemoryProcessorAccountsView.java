package com.itextos.beacon.inmemory.loader.process;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.itextos.beacon.commonlib.commondbpool.DBDataSourceFactory;
import com.itextos.beacon.inmemdata.account.dao.AccountInfo;

public class InmemoryProcessorAccountsView  {
    

    private static final Log log         = LogFactory.getLog(InmemoryProcessor.class);
    
    protected InmemoryInput  mInmemoryInput;
    private boolean          isFirstTime = true;
    
    // Pagination constants - can be made configurable
    private int DEFAULT_PAGE_SIZE = 100;
    private int DEFAULT_FETCH_SIZE = 100;
    
    AccountInfo inmemoryProcessor;
    
    public InmemoryProcessorAccountsView(InmemoryInput  mInmemoryInput,AccountInfo inmemoryProcessor) {
    	
    	this.mInmemoryInput=mInmemoryInput;
    	this.inmemoryProcessor=inmemoryProcessor;
    }
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
	 
	 public void doWithPagination() throws Exception {
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
    private boolean processPageWithNewConnection(int pageNumber) throws Exception {
        int pageSize = getPageSize();
        int offset = pageNumber * pageSize;
        
        // ✅ Enhanced pagination SQL for views
        String paginatedSQL = addPaginationToSQLWithOrderBy(mInmemoryInput.getSQL(), offset, pageSize);
        
        log.debug("Executing paginated SQL for view: " + paginatedSQL);
        
        try (Connection connection = DBDataSourceFactory.getConnection(mInmemoryInput.getJNDIInfo());
             PreparedStatement pstmt = connection.prepareStatement(paginatedSQL);
             ResultSet resultSet = pstmt.executeQuery()) {
            
            pstmt.setFetchSize(DEFAULT_FETCH_SIZE);
            
            int rowCount = 0;
            boolean hasData = false;
            
            // Process results
            while (resultSet.next()) {
               
            	inmemoryProcessor.processResultSet(resultSet);
                rowCount++;
                hasData = true;
            }
            
            log.debug("Page " + pageNumber + " returned " + rowCount + " rows");
            
            // If no data returned, it could be the end or an issue
            if (!hasData) {
                log.warn("No data returned for page " + pageNumber + ". Possible issues:");
                log.warn("1. View might be empty");
                log.warn("2. OFFSET might be beyond available records");
                log.warn("3. View definition might have internal limits");
            }
            
            return rowCount == pageSize; // More pages if we got a full page
            
        } catch (final SQLException e) {
            log.error("SQL Error on page " + pageNumber + ": " + e.getMessage());
            log.error("Failed SQL: " + paginatedSQL);
            diagnosePaginationIssue(pageNumber, pageSize, offset);
            return false;
        }
    }
    
    /**
     * Enhanced pagination that adds ORDER BY if missing
     */
    protected String addPaginationToSQLWithOrderBy(String originalSQL, int offset, int pageSize) {
        if (originalSQL == null || originalSQL.trim().isEmpty()) {
            return originalSQL;
        }
        
        String sqlUpper = originalSQL.toUpperCase().trim();
        
        // Check if SQL already has pagination
        if (sqlUpper.contains("LIMIT") || sqlUpper.contains("OFFSET")) {
            log.warn("SQL may already contain pagination clauses: " + originalSQL);
            return originalSQL;
        }
        
        StringBuilder paginatedSQL = new StringBuilder(originalSQL);
        
        // Remove trailing semicolon if present
        if (originalSQL.endsWith(";")) {
            paginatedSQL = new StringBuilder(originalSQL.substring(0, originalSQL.length() - 1));
        }
        
        // ✅ CRITICAL: Add ORDER BY if missing (required for consistent pagination)
        if (!sqlUpper.contains("ORDER BY")) {
            String primaryKey = null;
            if (primaryKey != null) {
                paginatedSQL.append(" ORDER BY ").append(primaryKey);
                log.info("Added ORDER BY " + primaryKey + " for consistent pagination");
            } else {
                // Fallback: use first column or rowid
                paginatedSQL.append(" ORDER BY 1"); // Order by first column
                log.warn("No primary key found, using ORDER BY 1 for pagination");
            }
        }
        
        // Add pagination
        paginatedSQL.append(" LIMIT ").append(pageSize).append(" OFFSET ").append(offset);
        
        return paginatedSQL.toString();
    }
    
 
    
    /**
     * Diagnostic method to identify pagination issues
     * @throws Exception 
     */
    private void diagnosePaginationIssue(int pageNumber, int pageSize, int offset) throws Exception {
        try (Connection connection = DBDataSourceFactory.getConnection(mInmemoryInput.getJNDIInfo())) {
            
            String originalSQL = mInmemoryInput.getSQL();
            
            // 1. Check if view returns any data at all
            String countSQL = "SELECT COUNT(*) as total FROM (" + originalSQL + ") as base_query";
            try (PreparedStatement countStmt = connection.prepareStatement(countSQL);
                 ResultSet countRs = countStmt.executeQuery()) {
                
                if (countRs.next()) {
                    int totalRows = countRs.getInt("total");
                    log.info("Total rows in view: " + totalRows);
                    
                    if (totalRows == 0) {
                        log.error("View is empty - no records to paginate");
                        return;
                    }
                    
                    if (offset >= totalRows) {
                        log.warn("OFFSET " + offset + " is beyond total rows " + totalRows);
                    }
                }
            }
            
            // 2. Check first page without OFFSET
            String firstPageSQL = originalSQL + " LIMIT 10";
            try (PreparedStatement testStmt = connection.prepareStatement(firstPageSQL);
                 ResultSet testRs = testStmt.executeQuery()) {
                
                if (testRs.next()) {
                    log.info("First page query returns data - pagination should work");
                } else {
                    log.error("Even first page returns no data - view might have issues");
                }
            }
            
            // 3. Check view definition
            log.info("Checking view definition...");
            String viewName = InmemoryProcessor.getFirstTableName(originalSQL);
            if (viewName != null) {
                checkViewDefinition(connection, viewName);
            }
            
        } catch (SQLException e) {
            log.error("Diagnostic failed: " + e.getMessage());
        }
    }
    
    private void checkViewDefinition(Connection connection, String viewName) {
        try {
            DatabaseMetaData metaData = connection.getMetaData();
            
            // Get view definition (database-specific)
            ResultSet views = metaData.getTables(null, null, viewName, new String[]{"VIEW"});
            if (views.next()) {
                log.info("View " + viewName + " exists");
                
                // Try to get view definition (MySQL/MariaDB)
                try (Statement stmt = connection.createStatement();
                     ResultSet rs = stmt.executeQuery("SHOW CREATE VIEW " + viewName)) {
                    
                    if (rs.next()) {
                        String createView = rs.getString(2);
                        log.info("View definition: " + createView);
                        
                        // Check for internal limits
                        if (createView.toUpperCase().contains("LIMIT")) {
                            log.error("VIEW HAS INTERNAL LIMIT - THIS BREAKS PAGINATION!");
                        }
                    }
                } catch (SQLException e) {
                    log.warn("Could not retrieve view definition: " + e.getMessage());
                }
            }
            
        } catch (SQLException e) {
            log.error("Error checking view definition: " + e.getMessage());
        }
    }
}