package com.itextos.beacon.logprocessor.controller;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.itextos.beacon.queryprocessor.commonutils.CommonVariables;
import com.itextos.beacon.queryprocessor.databaseconnector.DBConnectionProvider;
import com.itextos.beacon.queryprocessor.databaseconnector.ResultSetConverter;
import com.itextos.beacon.queryprocessor.databaseconnector.SQLStatementExecutor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * log data from MARIADB or POSTGRES
 */
@RestController
@RequestMapping("/log_queue/list")
public class LogDataQueueListController {

    private static final Log log = LogFactory.getLog(LogDataQueueListController.class);
    public static String API_NAME = CommonVariables.LOG_QUEUE_LIST_API;

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<JSONObject> getLogDataQueueList(@RequestBody String requestBody) {
        try {
            // Parse request
            RequestContext context = parseRequest(requestBody);
            if (context.hasError()) {
                return ResponseEntity.status(context.statusCode).body(createErrorResponse(context.errorMessage));
            }

            // Process queue list
            JSONObject result = processQueueList(context);
            return ResponseEntity.ok(result);

        } catch (Exception e) {
            log.error("Unexpected error occurred", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(createErrorResponse("Unable to retrieve Pending Queue List information"));
        }
    }

    private RequestContext parseRequest(String requestBody) {
        RequestContext context = new RequestContext();
        
        try {
            final JSONParser parser = new JSONParser();
            JSONObject reqJson = (JSONObject) parser.parse(requestBody);
            context.reqJson = reqJson;
            
        } catch (Exception e) {
            log.error("JSON Parsing Error", e);
            context.errorMessage = "Invalid JSON";
            context.statusCode = HttpStatus.BAD_REQUEST;
        }
        
        return context;
    }

    private JSONObject processQueueList(RequestContext context) {
        Connection masterDBConn = null;
        ResultSet rs_pending_queue = null;
        Statement stmt = null;
        
        try {
            // Get database connection
            masterDBConn = DBConnectionProvider.getMasterDBConnection();
            
            int pageNo = 1;
            if (context.reqJson.get(CommonVariables.PAGE_NO) != null) {
                pageNo = Integer.parseInt(String.valueOf(context.reqJson.get(CommonVariables.PAGE_NO)));
            }

            final JSONArray queList = new JSONArray();
            log.info("Getting queue list");

            // Execute the query and process results
            rs_pending_queue = SQLStatementExecutor.getQueueStatusList(masterDBConn, pageNo);
            
            final JSONArray ja = ResultSetConverter.ConvertRsToJSONArray(rs_pending_queue, null, "");
            if (ja != null) {
                queList.addAll(ja);
            }

            final JSONObject resJson = new JSONObject();
            resJson.put("queue_list", queList);
            resJson.put(CommonVariables.SERVER_TIMESTAMP, System.currentTimeMillis());
            
            return resJson;
            
        } catch (Exception e) {
            log.error("Error Occurred during queue list processing", e);
            return createErrorResponse("Unable to retrieve Pending Queue List information");
        } finally {
            // Close resources
            try {
                if (rs_pending_queue != null) {
                    stmt = rs_pending_queue.getStatement();
                    rs_pending_queue.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
                if (masterDBConn != null && !masterDBConn.isClosed()) {
                    masterDBConn.close();
                }
            } catch (Exception ex) {
                log.error("Error closing database resources", ex);
            }
        }
    }

    private JSONObject createErrorResponse(String message) {
        JSONObject errorResponse = new JSONObject();
        errorResponse.put(CommonVariables.STATUS_MESSAGE, message);
        errorResponse.put(CommonVariables.SERVER_TIMESTAMP, System.currentTimeMillis());
        return errorResponse;
    }

    // Helper class for request context management
    private static class RequestContext {
        JSONObject reqJson;
        String errorMessage;
        HttpStatus statusCode;

        boolean hasError() {
            return errorMessage != null;
        }
    }
}