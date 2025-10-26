package com.itextos.beacon.logprocessor.controller;

import java.sql.Connection;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.itextos.beacon.queryprocessor.commonutils.CommonVariables;
import com.itextos.beacon.queryprocessor.commonutils.Utility;
import com.itextos.beacon.queryprocessor.databaseconnector.ConnectionPoolSingleton;
import com.itextos.beacon.queryprocessor.databaseconnector.DBConnectionProvider;
import com.itextos.beacon.queryprocessor.databaseconnector.SQLStatementExecutor;
import com.itextos.beacon.queryprocessor.requestreceiver.QueryEngine;

/**
 * log data from MARIADB or POSTGRES
 */
@RestController
@RequestMapping("/log_queue/initiate")
public class CreateLogDataQueueController {

    private static final Log log = LogFactory.getLog(QueryEngine.class);
    public static String API_NAME = CommonVariables.LOG_QUEUE_CREATE_API;
    private static ConnectionPoolSingleton connPool = null;

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<JSONObject> createLogQueue(
            @RequestBody String requestBody,
            @RequestHeader Map<String, String> headers) {
        
        try {
            // Initialize connection pool if needed
            if (connPool == null) {
                connPool = ConnectionPoolSingleton.getInstance();
            }

            // Parse request
            RequestContext context = parseRequest(requestBody);
            if (context.hasError) {
                return ResponseEntity.badRequest().body(context.resJson);
            }

            // Validate request
            context = validateRequest(context);
            if (context.hasError) {
                return ResponseEntity.badRequest().body(context.resJson);
            }

            // Validate dates
            context = validateDates(context);
            if (context.hasError) {
                return ResponseEntity.badRequest().body(context.resJson);
            }

            // Validate sort field
            context = validateSortField(context);
            if (context.hasError) {
                return ResponseEntity.badRequest().body(context.resJson);
            }

            // Create queue
            JSONObject result = createQueue(context, headers);
            return ResponseEntity.ok(result);

        } catch (Exception e) {
            log.error("Unexpected error occurred", e);
            JSONObject errorResponse = new JSONObject();
            errorResponse.put(CommonVariables.SERVER_TIMESTAMP, System.currentTimeMillis());
            errorResponse.put(CommonVariables.STATUS_MESSAGE, "Internal server error");
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    private RequestContext parseRequest(String requestBody) {
        RequestContext context = new RequestContext(new JSONObject(), new JSONObject());
        
        try {
            final JSONParser parser = new JSONParser();
            JSONObject reqJson = (JSONObject) parser.parse(requestBody);
            
            context.reqJson = reqJson;
            context.resJson.put(CommonVariables.SERVER_TIMESTAMP, System.currentTimeMillis());
            
        } catch (Exception e) {
            log.error("JSON Parsing Error", e);
            context.resJson.put(CommonVariables.SERVER_TIMESTAMP, System.currentTimeMillis());
            context.resJson.put(CommonVariables.STATUS_MESSAGE, "Invalid JSON");
            context.hasError = true;
        }
        
        return context;
    }

    private RequestContext validateRequest(RequestContext context) {
        if (context.hasError) {
            return context;
        }

        try {
            if (!context.reqJson.containsKey(CommonVariables.R_PARAM)) {
                context.resJson.put(CommonVariables.STATUS_MESSAGE, "Missing required parameter: " + CommonVariables.R_PARAM);
                context.hasError = true;
            }
            return context;
        } catch (Exception e) {
            log.error("Error validating request", e);
            context.resJson.put(CommonVariables.STATUS_MESSAGE, "Error validating request");
            context.hasError = true;
            return context;
        }
    }

    private RequestContext validateDates(RequestContext context) {
        if (context.hasError) {
            return context;
        }

        try {
            final JSONObject paramJson = (JSONObject) context.reqJson.get(CommonVariables.R_PARAM);
            
            if (!paramJson.containsKey(CommonVariables.START_DATE) || !paramJson.containsKey(CommonVariables.END_DATE)) {
                context.resJson.put(CommonVariables.STATUS_MESSAGE, "Missing start_date or end_date");
                context.hasError = true;
                return context;
            }

            final String DATE_FORMAT_S = "yyyy-M-dd HH:mm:ss";
            final DateTimeFormatter formatWithS = DateTimeFormatter.ofPattern(DATE_FORMAT_S);
            final LocalDateTime paramStartTime = LocalDateTime
                    .parse(paramJson.get(CommonVariables.START_DATE).toString(), formatWithS);
            final LocalDateTime paramEndTime = LocalDateTime
                    .parse(paramJson.get(CommonVariables.END_DATE).toString(), formatWithS);

            final LocalDate ldt_cur_dt = LocalDate.now();

            // Validate start/end time order
            if (paramStartTime.isAfter(paramEndTime)) {
                log.info(String.format("Param Start Time: %s", Utility.formatDateTime(paramStartTime)));
                log.info(String.format("Param End Time: %s", Utility.formatDateTime(paramEndTime)));
                log.error("Start Time greater than End Time");
                context.resJson.put(CommonVariables.STATUS_MESSAGE, "Start Time greater than End Time");
                context.hasError = true;
                return context;
            }

            final LocalDate paramStartDate = paramStartTime.toLocalDate();
            final LocalDate paramEndDate = paramEndTime.toLocalDate();

            final int max_past_days = Utility.getInteger(QueryEngine.mySQL_cfg_val.getProperty("maxPastDays"));
            final LocalDate ldt_max_old_dt = paramStartDate.plusDays(-max_past_days);

            if (ldt_max_old_dt.isAfter(paramStartDate)) {
                log.info(String.format("Param Start Time: %s", Utility.formatDateTime(paramStartTime)));
                log.info(String.format("Param End Time: %s", Utility.formatDateTime(paramEndTime)));
                log.error("Start Time cannot be Older than " + max_past_days + " Days");
                context.resJson.put(CommonVariables.STATUS_MESSAGE,
                        "Start Time cannot be Older than " + max_past_days + " Days");
                context.hasError = true;
                return context;
            }

            return context;
        } catch (Exception e) {
            log.error("Error validating dates", e);
            context.resJson.put(CommonVariables.STATUS_MESSAGE, "Error processing date parameters");
            context.hasError = true;
            return context;
        }
    }

    private RequestContext validateSortField(RequestContext context) {
        if (context.hasError) {
            return context;
        }

        try {
            final JSONObject paramJson = (JSONObject) context.reqJson.get(CommonVariables.R_PARAM);

            if (paramJson.containsKey("sort_by")) {
                final String sort_field = Utility.nullCheck(paramJson.get("sort_by"), true);

                if (!connPool.hmLog_DL_Col_Map.containsKey(sort_field)) {
                    log.error("Invalid Sort Field: " + sort_field);
                    context.resJson.put(CommonVariables.STATUS_MESSAGE, "Invalid Sort Field: " + sort_field);
                    context.hasError = true;
                }
            }
            return context;
        } catch (Exception e) {
            log.error("Error validating sort field", e);
            context.resJson.put(CommonVariables.STATUS_MESSAGE, "Error validating sort field");
            context.hasError = true;
            return context;
        }
    }

    private JSONObject createQueue(RequestContext context, Map<String, String> headers) {
        if (context.hasError) {
            return context.resJson;
        }

        Connection masterDBConn = null;
        try {
            masterDBConn = DBConnectionProvider.getMasterDBConnection();
            final SQLStatementExecutor dml = new SQLStatementExecutor();
            
            log.info("Creating queue");
            final String queue_id = dml.CreateQueue(
                masterDBConn, 
                context.reqJson.get(CommonVariables.R_PARAM).toString(),
                context.reqJson.get(CommonVariables.R_APP).toString(),
                context.reqJson.get(CommonVariables.R_APP_VERSION).toString(),
                context.reqJson.get(CommonVariables.R_USERNAME).toString(), 
                headers.get("HOST"),
                CommonVariables.QUEUED
            );

            context.resJson.put("queue_id", queue_id);
            log.info("Queue created successfully. Queue ID: " + queue_id);
            return context.resJson;
            
        } catch (Exception e) {
            log.error("Error Occurred", e);
            context.resJson.put(CommonVariables.STATUS_MESSAGE, "Problem when creating the queue");
            return context.resJson;
        } finally {
            if (masterDBConn != null) {
                try {
                    if (!masterDBConn.isClosed()) {
                        masterDBConn.close();
                    }
                } catch (Exception ex) {
                    log.error("Error while closing Master DB Connection", ex);
                }
            }
        }
    }

    // Helper class to pass both request and response JSON objects through the processing chain
    private static class RequestContext {
        JSONObject reqJson;
        JSONObject resJson;
        boolean hasError;

        RequestContext(JSONObject reqJson, JSONObject resJson) {
            this.reqJson = reqJson;
            this.resJson = resJson;
            this.hasError = false;
        }
    }
}