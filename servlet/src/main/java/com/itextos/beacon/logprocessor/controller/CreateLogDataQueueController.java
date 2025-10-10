package com.itextos.beacon.logprocessor.controller;

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
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;

import com.itextos.beacon.queryprocessor.commonutils.CommonVariables;
import com.itextos.beacon.queryprocessor.commonutils.Utility;
import com.itextos.beacon.queryprocessor.databaseconnector.ConnectionPoolSingleton;
import com.itextos.beacon.queryprocessor.databaseconnector.DBConnectionProvider;
import com.itextos.beacon.queryprocessor.databaseconnector.SQLStatementExecutor;
import com.itextos.beacon.queryprocessor.requestreceiver.QueryEngine;

import reactor.core.publisher.Mono;

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
    public Mono<JSONObject> createLogQueue(
            @RequestBody String requestBody,
            @RequestHeader Map<String, String> headers,
            ServerWebExchange exchange) {
        
        return parseRequest(requestBody, exchange)
            .flatMap(context -> validateRequest(context, exchange))
            .flatMap(context -> validateDates(context, exchange))
            .flatMap(context -> validateSortField(context, exchange))
            .flatMap(context -> createQueue(context, headers, exchange))
            .onErrorResume(e -> {
                log.error("Unexpected error occurred", e);
                JSONObject errorResponse = new JSONObject();
                errorResponse.put(CommonVariables.SERVER_TIMESTAMP, System.currentTimeMillis());
                errorResponse.put(CommonVariables.STATUS_MESSAGE, "Internal server error");
                exchange.getResponse().setStatusCode(HttpStatus.INTERNAL_SERVER_ERROR);
                return Mono.just(errorResponse);
            });
    }

    private Mono<RequestContext> parseRequest(String requestBody, ServerWebExchange exchange) {
        return Mono.fromCallable(() -> {
            if (connPool == null) {
                connPool = ConnectionPoolSingleton.getInstance();
            }

            final JSONObject resJson = new JSONObject();
            resJson.put(CommonVariables.SERVER_TIMESTAMP, System.currentTimeMillis());

            final JSONParser parser = new JSONParser();
            JSONObject reqJson = (JSONObject) parser.parse(requestBody);

            return new RequestContext(reqJson, resJson);
        })
        .onErrorResume(e -> {
            log.error("JSON Parsing Error", e);
            JSONObject errorResponse = new JSONObject();
            errorResponse.put(CommonVariables.SERVER_TIMESTAMP, System.currentTimeMillis());
            errorResponse.put(CommonVariables.STATUS_MESSAGE, "Invalid JSON");
            exchange.getResponse().setStatusCode(HttpStatus.BAD_REQUEST);
            // Return a RequestContext with error response
            return Mono.just(new RequestContext(new JSONObject(), errorResponse, true));
        });
    }

    private Mono<RequestContext> validateRequest(RequestContext context, ServerWebExchange exchange) {
        if (context.hasError) {
            return Mono.just(context);
        }

        try {
            if (!context.reqJson.containsKey(CommonVariables.R_PARAM)) {
                context.resJson.put(CommonVariables.STATUS_MESSAGE, "Missing required parameter: " + CommonVariables.R_PARAM);
                exchange.getResponse().setStatusCode(HttpStatus.BAD_REQUEST);
                context.hasError = true;
            }
            return Mono.just(context);
        } catch (Exception e) {
            log.error("Error validating request", e);
            context.resJson.put(CommonVariables.STATUS_MESSAGE, "Error validating request");
            exchange.getResponse().setStatusCode(HttpStatus.BAD_REQUEST);
            context.hasError = true;
            return Mono.just(context);
        }
    }

    private Mono<RequestContext> validateDates(RequestContext context, ServerWebExchange exchange) {
        if (context.hasError) {
            return Mono.just(context);
        }

        try {
            final JSONObject paramJson = (JSONObject) context.reqJson.get(CommonVariables.R_PARAM);
            
            if (!paramJson.containsKey(CommonVariables.START_DATE) || !paramJson.containsKey(CommonVariables.END_DATE)) {
                context.resJson.put(CommonVariables.STATUS_MESSAGE, "Missing start_date or end_date");
                exchange.getResponse().setStatusCode(HttpStatus.BAD_REQUEST);
                context.hasError = true;
                return Mono.just(context);
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
                exchange.getResponse().setStatusCode(HttpStatus.BAD_REQUEST);
                context.resJson.put(CommonVariables.STATUS_MESSAGE, "Start Time greater than End Time");
                context.hasError = true;
                return Mono.just(context);
            }

            final LocalDate paramStartDate = paramStartTime.toLocalDate();
            final LocalDate paramEndDate = paramEndTime.toLocalDate();

            final int max_past_days = Utility.getInteger(QueryEngine.mySQL_cfg_val.getProperty("maxPastDays"));
            final LocalDate ldt_max_old_dt = paramStartDate.plusDays(-max_past_days);

            if (ldt_max_old_dt.isAfter(paramStartDate)) {
                log.info(String.format("Param Start Time: %s", Utility.formatDateTime(paramStartTime)));
                log.info(String.format("Param End Time: %s", Utility.formatDateTime(paramEndTime)));
                log.error("Start Time cannot be Older than " + max_past_days + " Days");
                exchange.getResponse().setStatusCode(HttpStatus.BAD_REQUEST);
                context.resJson.put(CommonVariables.STATUS_MESSAGE,
                        "Start Time cannot be Older than " + max_past_days + " Days");
                context.hasError = true;
                return Mono.just(context);
            }

            return Mono.just(context);
        } catch (Exception e) {
            log.error("Error validating dates", e);
            context.resJson.put(CommonVariables.STATUS_MESSAGE, "Error processing date parameters");
            exchange.getResponse().setStatusCode(HttpStatus.BAD_REQUEST);
            context.hasError = true;
            return Mono.just(context);
        }
    }

    private Mono<RequestContext> validateSortField(RequestContext context, ServerWebExchange exchange) {
        if (context.hasError) {
            return Mono.just(context);
        }

        try {
            final JSONObject paramJson = (JSONObject) context.reqJson.get(CommonVariables.R_PARAM);

            if (paramJson.containsKey("sort_by")) {
                final String sort_field = Utility.nullCheck(paramJson.get("sort_by"), true);

                if (!connPool.hmLog_DL_Col_Map.containsKey(sort_field)) {
                    log.error("Invalid Sort Field: " + sort_field);
                    exchange.getResponse().setStatusCode(HttpStatus.BAD_REQUEST);
                    context.resJson.put(CommonVariables.STATUS_MESSAGE, "Invalid Sort Field: " + sort_field);
                    context.hasError = true;
                }
            }
            return Mono.just(context);
        } catch (Exception e) {
            log.error("Error validating sort field", e);
            context.resJson.put(CommonVariables.STATUS_MESSAGE, "Error validating sort field");
            exchange.getResponse().setStatusCode(HttpStatus.BAD_REQUEST);
            context.hasError = true;
            return Mono.just(context);
        }
    }

    private Mono<JSONObject> createQueue(RequestContext context, Map<String, String> headers, ServerWebExchange exchange) {
        if (context.hasError) {
            return Mono.just(context.resJson);
        }

        return Mono.usingWhen(
            Mono.fromCallable(() -> DBConnectionProvider.getMasterDBConnection()),
            masterDBConn -> Mono.fromCallable(() -> {
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
            }),
            masterDBConn -> Mono.fromRunnable(() -> {
                try {
                    if (!masterDBConn.isClosed()) {
                        masterDBConn.close();
                    }
                } catch (Exception ex) {
                    log.error("Error while closing Master DB Connection", ex);
                }
            })
        )
        .onErrorResume(e -> {
            log.error("Error Occurred", e);
            exchange.getResponse().setStatusCode(HttpStatus.INTERNAL_SERVER_ERROR);
            context.resJson.put(CommonVariables.STATUS_MESSAGE, "Problem when creating the queue");
            return Mono.just(context.resJson);
        });
    }

    // Helper class to pass both request and response JSON objects through the reactive chain
    private static class RequestContext {
        final JSONObject reqJson;
        final JSONObject resJson;
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