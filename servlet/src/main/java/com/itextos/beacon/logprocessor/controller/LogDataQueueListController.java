package com.itextos.beacon.logprocessor.controller;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;

import com.itextos.beacon.queryprocessor.commonutils.CommonVariables;
import com.itextos.beacon.queryprocessor.databaseconnector.DBConnectionProvider;
import com.itextos.beacon.queryprocessor.databaseconnector.ResultSetConverter;
import com.itextos.beacon.queryprocessor.databaseconnector.SQLStatementExecutor;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

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

    @PostMapping( consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<JSONObject> getLogDataQueueList(@RequestBody String requestBody, ServerWebExchange exchange) {
        return parseRequest(requestBody, exchange)
                .flatMap(requestContext -> processQueueList(requestContext, exchange))
                .onErrorResume(e -> {
                    log.error("Unexpected error occurred", e);
                    return createErrorResponse("Unable to retrieve Pending Queue List information", 
                            HttpStatus.INTERNAL_SERVER_ERROR, exchange);
                });
    }

    private Mono<RequestContext> parseRequest(String requestBody, ServerWebExchange exchange) {
        return Mono.fromCallable(() -> {
            final JSONParser parser = new JSONParser();
            JSONObject reqJson = (JSONObject) parser.parse(requestBody);
            return new RequestContext(reqJson);
        })
        .onErrorResume(e -> {
            log.error("JSON Parsing Error", e);
            return createErrorContext("Invalid JSON", HttpStatus.BAD_REQUEST, exchange);
        });
    }

    private Mono<JSONObject> processQueueList(RequestContext context, ServerWebExchange exchange) {
        return Mono.usingWhen(
            // Resource supplier - get database connection
            Mono.fromCallable(() -> DBConnectionProvider.getMasterDBConnection())
                .subscribeOn(Schedulers.boundedElastic()),
            
            // Resource usage - process the queue list
            masterDBConn -> Mono.fromCallable(() -> {
                int pageNo = 1;
                if (context.reqJson.get(CommonVariables.PAGE_NO) != null) {
                    pageNo = Integer.parseInt(String.valueOf(context.reqJson.get(CommonVariables.PAGE_NO)));
                }

                final JSONArray queList = new JSONArray();
                log.info("Getting queue list");

                // Execute the query and process results
                final ResultSet rs_pending_queue = SQLStatementExecutor.getQueueStatusList(masterDBConn, pageNo);
                
                try {
                    final JSONArray ja = ResultSetConverter.ConvertRsToJSONArray(rs_pending_queue, null, "");
                    if (ja != null) {
                        queList.addAll(ja);
                    }

                    final JSONObject resJson = new JSONObject();
                    resJson.put("queue_list", queList);
                    resJson.put(CommonVariables.SERVER_TIMESTAMP, System.currentTimeMillis());
                    
                    exchange.getResponse().setStatusCode(HttpStatus.OK);
                    return resJson;
                    
                } finally {
                    // Close ResultSet and Statement
                    if (rs_pending_queue != null) {
                        try {
                            Statement stmt = rs_pending_queue.getStatement();
                            rs_pending_queue.close();
                            if (stmt != null) {
                                stmt.close();
                            }
                        } catch (Exception e) {
                            log.error("Error closing ResultSet or Statement", e);
                        }
                    }
                }
            })
            .subscribeOn(Schedulers.boundedElastic()),
            
            // Resource cleanup - close database connection
            masterDBConn -> Mono.fromRunnable(() -> {
                try {
                    if (masterDBConn != null && !masterDBConn.isClosed()) {
                        masterDBConn.close();
                    }
                } catch (Exception ex) {
                    log.error("Error while closing Master DB Connection", ex);
                }
            })
            .subscribeOn(Schedulers.boundedElastic())
        )
        .onErrorResume(e -> {
            log.error("Error Occurred during queue list processing", e);
            return createErrorResponse("Unable to retrieve Pending Queue List information", 
                    HttpStatus.INTERNAL_SERVER_ERROR, exchange);
        });
    }

    private Mono<RequestContext> createErrorContext(String message, HttpStatus status, ServerWebExchange exchange) {
        return Mono.fromCallable(() -> {
            RequestContext context = new RequestContext(null);
            context.errorMessage = message;
            context.statusCode = status;
            exchange.getResponse().setStatusCode(status);
            return context;
        });
    }

    private Mono<JSONObject> createErrorResponse(String message, HttpStatus status, ServerWebExchange exchange) {
        return Mono.fromCallable(() -> {
            JSONObject errorResponse = new JSONObject();
            errorResponse.put(CommonVariables.STATUS_MESSAGE, message);
            errorResponse.put(CommonVariables.SERVER_TIMESTAMP, System.currentTimeMillis());
            exchange.getResponse().setStatusCode(status);
            return errorResponse;
        });
    }

    // Helper class for request context management
    private static class RequestContext {
        final JSONObject reqJson;
        String errorMessage;
        HttpStatus statusCode;

        RequestContext(JSONObject reqJson) {
            this.reqJson = reqJson;
        }

        boolean hasError() {
            return errorMessage != null;
        }
    }
}