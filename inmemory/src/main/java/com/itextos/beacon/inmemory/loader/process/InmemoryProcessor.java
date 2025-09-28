package com.itextos.beacon.inmemory.loader.process;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.itextos.beacon.commonlib.commondbpool.DBDataSourceFactory;
import com.itextos.beacon.inmemory.inmemdata.mccmnc.MccMncCollection;

public abstract class InmemoryProcessor
        implements
        IInmemoryProcess
{

    private static final Log log         = LogFactory.getLog(InmemoryProcessor.class);

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
    	
    	if(this instanceof MccMncCollection) {
    		
    	       doWithPagenation();

    	}else{
    	       doWithoutPagenation();

    	}
       
    }

    private void doWithoutPagenation() {
		


        try (
                Connection con = DBDataSourceFactory.getConnection(mInmemoryInput.getJNDIInfo());
                PreparedStatement pstmt = con.prepareStatement(mInmemoryInput.getSQL(), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                ResultSet mResultSet = pstmt.executeQuery();)
        {
        //    pstmt.setFetchSize(1000);
            processResultSet(mResultSet);
            isFirstTime = false;
        }
        catch (final Exception e)
        {
            log.error("ignorable Exception. Exception while doinng inmemory load of '" + mInmemoryInput.getInmemoryId() + "'", e);

            if (isFirstTime)
            {
                log.error("Since the initial load has failed, stopping the application for " + mInmemoryInput, e);
                System.exit(-9);
            }
        }

		
	}

	protected void doWithPagenation() {
	    int pageNumber = 0;
	    boolean hasMoreData = true;
	    
	    // ✅ Single connection for all pages
	    try (Connection connection = DBDataSourceFactory.getConnection(mInmemoryInput.getJNDIInfo())) {
	        
	        while (hasMoreData) {
	            // ✅ Process page and check if more data exists
	            hasMoreData = processPage(connection, pageNumber);
	            pageNumber++;
	            
	            log.debug("Processed page " + pageNumber + ", hasMoreData: " + hasMoreData);
	        }
	        
	        isFirstTime = false;
	        log.info("Completed processing all pages. Total pages: " + pageNumber);
	        
	    } catch (final Exception e) {
	        log.error("Exception during inmemory load of '" + mInmemoryInput.getInmemoryId() + "'", e);
	        
	        if (isFirstTime) {
	            log.error("Initial load failed, stopping application", e);
	            System.exit(-9);
	        }
	    }}

	private boolean processPage(Connection connection, int pageNumber) {
	    int pageSize = getPageSize();
	    int offset = pageNumber * pageSize;
	    
	    String paginatedSQL = addPaginationToSQL(mInmemoryInput.getSQL(), offset, pageSize);
	    
	    try (PreparedStatement pstmt = connection.prepareStatement(paginatedSQL, 
	            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
	         ResultSet resultSet = pstmt.executeQuery()) {
	        
	        pstmt.setFetchSize(getFetchSize());
	        
	        int rowCount = 0;
	        boolean hasData = false;
	        
	        // Process results and count rows
	        while (resultSet.next()) {
	            processSingleRow(resultSet); // Or your processing logic
	            rowCount++;
	            hasData = true;
	        }
	        
	        // ✅ Proper pagination logic:
	        // - If we got a full page, there might be more data
	        // - If we got less than a full page, this is the last page
	        return rowCount == pageSize;
	        
	    } catch (final SQLException e) {
	        log.error("Error processing page " + pageNumber + " for inmemory load of '" + 
	                 mInmemoryInput.getInmemoryId() + "'", e);
	        throw new RuntimeException("Failed to process page " + pageNumber, e);
	    }
	}
    
    /**
     * Adds pagination to the SQL query based on database type
     * @param originalSQL The original SQL query
     * @param offset The offset for pagination
     * @param pageSize The page size
     * @return Paginated SQL query
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
        
        // Add pagination - using LIMIT/OFFSET syntax (works for MySQL, PostgreSQL, SQLite, etc.)
        paginatedSQL.append(" LIMIT ").append(pageSize).append(" OFFSET ").append(offset);
        
        log.debug("Paginated SQL: " + paginatedSQL.toString());
        return paginatedSQL.toString();
    }
    
    /**
     * Alternative database-specific pagination method
     */
    protected String addPaginationToSQL(String originalSQL, int offset, int pageSize, String databaseType) {
        if (originalSQL == null || originalSQL.trim().isEmpty()) {
            return originalSQL;
        }
        
        String sqlUpper = originalSQL.toUpperCase().trim();
        
        // Check for existing pagination
        if (sqlUpper.contains("LIMIT") || sqlUpper.contains("OFFSET") || 
            sqlUpper.contains("ROWNUM") || sqlUpper.contains("FETCH")) {
            return originalSQL;
        }
        
        StringBuilder paginatedSQL = new StringBuilder(originalSQL);
        
        // Remove trailing semicolon if present
        if (originalSQL.endsWith(";")) {
            paginatedSQL = new StringBuilder(originalSQL.substring(0, originalSQL.length() - 1));
        }
        
        // Database-specific pagination
        if ("ORACLE".equalsIgnoreCase(databaseType)) {
            // Oracle pagination using ROWNUM
            return "SELECT * FROM (" +
                   "    SELECT a.*, ROWNUM rnum FROM (" + originalSQL + ") a " +
                   "    WHERE ROWNUM <= " + (offset + pageSize) +
                   ") WHERE rnum > " + offset;
        }
        else if ("SQLSERVER".equalsIgnoreCase(databaseType)) {
            // SQL Server 2012+ pagination
            if (!sqlUpper.contains("ORDER BY")) {
                // SQL Server requires ORDER BY for OFFSET/FETCH
                paginatedSQL.append(" ORDER BY (SELECT NULL)");
            }
            paginatedSQL.append(" OFFSET ").append(offset).append(" ROWS FETCH NEXT ").append(pageSize).append(" ROWS ONLY");
        }
        else {
            // Default: MySQL, PostgreSQL, SQLite, etc.
            paginatedSQL.append(" LIMIT ").append(pageSize).append(" OFFSET ").append(offset);
        }
        
        return paginatedSQL.toString();
    }

    /**
     * Process a single row from the result set
     * @param resultSet The result set positioned at the current row
     * @throws SQLException If database error occurs
     */
    protected void processSingleRow(ResultSet resultSet) throws SQLException {
        // Default implementation - subclasses can override for row-by-row processing
        // while still benefiting from pagination
        // This method is called from processResultSet for each row
    }

    /**
     * Process the entire result set (maintains backward compatibility)
     * Subclasses can override processSingleRow for per-row processing
     * or override this method for bulk processing per page
     */
    protected abstract void processResultSet(ResultSet mResultSet) throws SQLException;

    /**
     * Get the page size for pagination
     * @return page size
     */
    protected int getPageSize() {
        // Can be made configurable via properties or environment variables
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
     * @return fetch size
     */
    protected int getFetchSize() {
        // Can be made configurable via properties or environment variables
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
        // This would depend on the EJB service's pagination capabilities
    }
    
    @Override
    public void refreshInmemoryData()
    {
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
}