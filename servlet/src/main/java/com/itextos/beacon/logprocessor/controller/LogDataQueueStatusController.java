package com.itextos.beacon.logprocessor.controller;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import com.itextos.beacon.queryprocessor.commonutils.Utility;
import com.itextos.beacon.queryprocessor.databaseconnector.DBConnectionProvider;
import com.itextos.beacon.queryprocessor.databaseconnector.ResultSetConverter;
import com.itextos.beacon.queryprocessor.databaseconnector.SQLStatementExecutor;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

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
    public Mono<JSONObject> getLogDataQueueStatus(@RequestBody String requestBody, ServerWebExchange exchange) {
        return parseRequest(requestBody, exchange)
                .flatMap(context -> validateQueueId(context, exchange))
                .flatMap(context -> retrieveQueueStatus(context, exchange))
                .onErrorResume(e -> {
                    log.error("Unexpected error occurred", e);
                    return createErrorResponse("Unable to retrieve Queue information", 
                            HttpStatus.INTERNAL_SERVER_ERROR, exchange);
                });
    }

    private Mono<RequestContext> parseRequest(String requestBody, ServerWebExchange exchange) {
        return Mono.fromCallable(() -> {
            final JSONParser parser = new JSONParser();
            JSONObject reqJson = (JSONObject) parser.parse(requestBody);
            
            JSONObject resJson = new JSONObject();
            resJson.put(CommonVariables.SERVER_TIMESTAMP, System.currentTimeMillis());
            
            return new RequestContext(reqJson, resJson);
        })
        .onErrorResume(e -> {
            log.error("JSON Parsing Error", e);
            JSONObject errorResponse = new JSONObject();
            errorResponse.put(CommonVariables.SERVER_TIMESTAMP, System.currentTimeMillis());
            errorResponse.put(CommonVariables.STATUS_MESSAGE, "Invalid JSON");
            exchange.getResponse().setStatusCode(HttpStatus.BAD_REQUEST);
            return Mono.just(new RequestContext(null, errorResponse, true));
        });
    }

    private Mono<RequestContext> validateQueueId(RequestContext context, ServerWebExchange exchange) {
        if (context.hasError) {
            return Mono.just(context);
        }

        return Mono.fromCallable(() -> {
            final String queue_id = Utility.nullCheck(context.reqJson.get(CommonVariables.QUEUE_ID), true);

            if ("".equals(queue_id)) {
                final String errMsg = "Parameter " + CommonVariables.QUEUE_ID + " not found";
                log.error(errMsg);
                context.resJson.put(CommonVariables.STATUS_MESSAGE, errMsg);
                exchange.getResponse().setStatusCode(HttpStatus.BAD_REQUEST);
                context.hasError = true;
                return context;
            }

            if (!queid_pattern.matcher(queue_id).matches()) {
                final String errMsg = "Invalid format of " + CommonVariables.QUEUE_ID + ": " + queue_id;
                log.error(errMsg);
                context.resJson.put(CommonVariables.STATUS_MESSAGE, errMsg);
                exchange.getResponse().setStatusCode(HttpStatus.BAD_REQUEST);
                context.hasError = true;
                return context;
            }

            context.queueId = queue_id;
            return context;
        });
    }

    private Mono<JSONObject> retrieveQueueStatus(RequestContext context, ServerWebExchange exchange) {
        if (context.hasError) {
            return Mono.just(context.resJson);
        }

        return Mono.usingWhen(
            // Resource supplier - get database connection
            Mono.fromCallable(() -> DBConnectionProvider.getMasterDBConnection())
                .subscribeOn(Schedulers.boundedElastic()),
            
            // Resource usage - retrieve queue status
            masterDBConn -> Mono.fromCallable(() -> {
                final ResultSet rs_queue_status = SQLStatementExecutor.getQueueStatusInfo(masterDBConn, context.queueId);
                
                try {
                    final JSONObject que_status = ResultSetConverter.ConvertRsToJSONObject(rs_queue_status, null, "");

                    // Close ResultSet and Statement
                    Statement stmt = rs_queue_status.getStatement();
                    rs_queue_status.close();
                    if (stmt != null) {
                        stmt.close();
                    }

                    if (que_status == null) {
                        final String errMsg = CommonVariables.QUEUE_ID + ": " + context.queueId + " not found";
                        log.info(errMsg);
                        context.resJson.put(CommonVariables.STATUS_MESSAGE, errMsg);
                        exchange.getResponse().setStatusCode(HttpStatus.BAD_REQUEST);
                    } else {
                        context.resJson.put("queue_status", que_status);
                        exchange.getResponse().setStatusCode(HttpStatus.OK);
                    }
                    
                    return context.resJson;
                    
                } catch (Exception e) {
                    // Ensure resources are closed even if conversion fails
                    try {
                        if (rs_queue_status != null && !rs_queue_status.isClosed()) {
                            Statement stmt = rs_queue_status.getStatement();
                            rs_queue_status.close();
                            if (stmt != null) {
                                stmt.close();
                            }
                        }
                    } catch (Exception ex) {
                        log.error("Error closing database resources", ex);
                    }
                    throw e;
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
            log.error("Error Occurred during queue status retrieval", e);
            JSONObject errorResponse = new JSONObject();
            errorResponse.put(CommonVariables.SERVER_TIMESTAMP, System.currentTimeMillis());
            errorResponse.put(CommonVariables.STATUS_MESSAGE, "Unable to retrieve Queue information");
            exchange.getResponse().setStatusCode(HttpStatus.INTERNAL_SERVER_ERROR);
            return Mono.just(errorResponse);
        });
    }

    private Mono<JSONObject> createErrorResponse(String message, HttpStatus status, ServerWebExchange exchange) {
        return Mono.fromCallable(() -> {
            JSONObject errorResponse = new JSONObject();
            errorResponse.put(CommonVariables.SERVER_TIMESTAMP, System.currentTimeMillis());
            errorResponse.put(CommonVariables.STATUS_MESSAGE, message);
            exchange.getResponse().setStatusCode(status);
            return errorResponse;
        });
    }

    // Helper class for request context management
    private static class RequestContext {
        final JSONObject reqJson;
        final JSONObject resJson;
        String queueId;
        boolean hasError;

        RequestContext(JSONObject reqJson, JSONObject resJson) {
            this.reqJson = reqJson;
            this.resJson = resJson;
            this.hasError = false;
        }

        RequestContext(JSONObject reqJson, JSONObject resJson, boolean hasError) {
            this.reqJson = reqJson;
            this.resJson = resJson;
            this.hasError = hasError;
        }
    }
}