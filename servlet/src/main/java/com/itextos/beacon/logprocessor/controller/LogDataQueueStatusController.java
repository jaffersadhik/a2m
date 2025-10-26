package com.itextos.beacon.logprocessor.controller;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import com.itextos.beacon.queryprocessor.commonutils.Utility;
import com.itextos.beacon.queryprocessor.databaseconnector.DBConnectionProvider;
import com.itextos.beacon.queryprocessor.databaseconnector.ResultSetConverter;
import com.itextos.beacon.queryprocessor.databaseconnector.SQLStatementExecutor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.regex.Pattern;

/**
 * log data from MARIADB or POSTGRES
 */
@RestController
@RequestMapping("/log_queue/status")
public class LogDataQueueStatusController {

    private static final Log log = LogFactory.getLog(LogDataQueueStatusController.class);
    public static String API_NAME = CommonVariables.LOG_QUEUE_STATUS_API;
    static Pattern queid_pattern = Pattern
            .compile("\\b[0-9a-f]{8}\\b-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-\\b[0-9a-f]{12}\\b");

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<JSONObject> getLogDataQueueStatus(@RequestBody String requestBody) {
        try {
            // Parse request
            RequestContext context = parseRequest(requestBody);
            if (context.hasError) {
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(context.resJson);
            }

            // Validate queue ID
            context = validateQueueId(context);
            if (context.hasError) {
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(context.resJson);
            }

            // Retrieve queue status
            JSONObject result = retrieveQueueStatus(context);
            return ResponseEntity.ok(result);

        } catch (Exception e) {
            log.error("Unexpected error occurred", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(createErrorResponse("Unable to retrieve Queue information"));
        }
    }

    private RequestContext parseRequest(String requestBody) {
        RequestContext context = new RequestContext();
        context.resJson = new JSONObject();
        context.resJson.put(CommonVariables.SERVER_TIMESTAMP, System.currentTimeMillis());

        try {
            final JSONParser parser = new JSONParser();
            JSONObject reqJson = (JSONObject) parser.parse(requestBody);
            context.reqJson = reqJson;
            
        } catch (Exception e) {
            log.error("JSON Parsing Error", e);
            context.resJson.put(CommonVariables.STATUS_MESSAGE, "Invalid JSON");
            context.hasError = true;
        }
        
        return context;
    }

    private RequestContext validateQueueId(RequestContext context) {
        if (context.hasError) {
            return context;
        }

        try {
            final String queue_id = Utility.nullCheck(context.reqJson.get(CommonVariables.QUEUE_ID), true);

            if ("".equals(queue_id)) {
                final String errMsg = "Parameter " + CommonVariables.QUEUE_ID + " not found";
                log.error(errMsg);
                context.resJson.put(CommonVariables.STATUS_MESSAGE, errMsg);
                context.hasError = true;
                return context;
            }

            if (!queid_pattern.matcher(queue_id).matches()) {
                final String errMsg = "Invalid format of " + CommonVariables.QUEUE_ID + ": " + queue_id;
                log.error(errMsg);
                context.resJson.put(CommonVariables.STATUS_MESSAGE, errMsg);
                context.hasError = true;
                return context;
            }

            context.queueId = queue_id;
            return context;
        } catch (Exception e) {
            log.error("Error validating queue ID", e);
            context.resJson.put(CommonVariables.STATUS_MESSAGE, "Error validating queue ID");
            context.hasError = true;
            return context;
        }
    }

    private JSONObject retrieveQueueStatus(RequestContext context) {
        Connection masterDBConn = null;
        ResultSet rs_queue_status = null;
        Statement stmt = null;
        
        try {
            // Get database connection
            masterDBConn = DBConnectionProvider.getMasterDBConnection();
            
            // Retrieve queue status
            rs_queue_status = SQLStatementExecutor.getQueueStatusInfo(masterDBConn, context.queueId);
            
            final JSONObject que_status = ResultSetConverter.ConvertRsToJSONObject(rs_queue_status, null, "");

            if (que_status == null) {
                final String errMsg = CommonVariables.QUEUE_ID + ": " + context.queueId + " not found";
                log.info(errMsg);
                context.resJson.put(CommonVariables.STATUS_MESSAGE, errMsg);
                return context.resJson;
            } else {
                context.resJson.put("queue_status", que_status);
                return context.resJson;
            }
            
        } catch (Exception e) {
            log.error("Error Occurred during queue status retrieval", e);
            return createErrorResponse("Unable to retrieve Queue information");
        } finally {
            // Close resources
            try {
                if (rs_queue_status != null && !rs_queue_status.isClosed()) {
                    stmt = rs_queue_status.getStatement();
                    rs_queue_status.close();
                    if (stmt != null) {
                        stmt.close();
                    }
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
        errorResponse.put(CommonVariables.SERVER_TIMESTAMP, System.currentTimeMillis());
        errorResponse.put(CommonVariables.STATUS_MESSAGE, message);
        return errorResponse;
    }

    // Helper class for request context management
    private static class RequestContext {
        JSONObject reqJson;
        JSONObject resJson;
        String queueId;
        boolean hasError;

        RequestContext() {
            this.hasError = false;
        }
    }
}