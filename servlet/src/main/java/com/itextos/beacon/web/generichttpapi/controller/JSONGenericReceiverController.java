package com.itextos.beacon.web.generichttpapi.controller;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.itextos.beacon.commonlib.constants.MiddlewareConstant;
import com.itextos.beacon.commonlib.utility.ClientIP;
import com.itextos.beacon.http.generichttpapi.common.utils.InterfaceInputParameters;
import com.itextos.beacon.http.generichttpapi.common.utils.Utility;
import com.itextos.beacon.http.interfaceutil.MessageSource;
import com.itextos.beacon.interfaces.generichttpapi.processor.reader.JSONRequestReader;
import com.itextos.beacon.interfaces.generichttpapi.processor.reader.RequestReader;
import com.itextos.beacon.smslog.JSONReceiverLog;
import com.itextos.beacon.smslog.TimeTakenInterfaceLog;

import jakarta.servlet.http.HttpServletRequest;

@RestController
@RequestMapping("/genericapi/JSONGenericReceiver")
public class JSONGenericReceiverController {

    private static final Log log = LogFactory.getLog(JSONGenericReceiverController.class);

    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> handleJsonGetRequest(
            @RequestHeader(value = "X-Forwarded-For", required = false) String clientIp,
            @RequestHeader(value = "Authorization", required = false) String authorization,
            HttpServletRequest request) {
        
        return processJsonRequest("GET", "", clientIp, authorization, request);
    }

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> handleJsonPostRequest(
            @RequestBody String requestBody,
            @RequestHeader(value = "X-Forwarded-For", required = false) String clientIp,
            @RequestHeader(value = "Authorization", required = false) String authorization,
            HttpServletRequest request) {
        
        return processJsonRequest("POST", requestBody, clientIp, authorization, request);
    }

    private ResponseEntity<String> processJsonRequest(String method, String requestBody, String clientIp, 
                                                     String authorization, HttpServletRequest request) {
        final Instant startTime = Instant.now();
        final StringBuffer logBuffer = new StringBuffer();
        
        logBuffer.append("\n##########################################\n")
                .append("JSONGenericReceiver request received in ").append(method).append("\n");

        if (log.isDebugEnabled()) {
            log.debug("JSON request received via " + method);
        }

        try {
            Map<String, String> params = new HashMap<>();
            
            params.put(MiddlewareConstant.MW_CLIENT_SOURCE_IP.getKey(), ClientIP.getClientIpAddress(clientIp, request));
            params.put(InterfaceInputParameters.AUTHORIZATION, authorization);
            params.put("http_request_body", requestBody);

            // Process request synchronously
            final RequestReader reader = new JSONRequestReader(params, method, MessageSource.GENERIC_JSON, new StringBuffer());

            String response;
            if ("GET".equalsIgnoreCase(method)) {
                response = reader.processGetRequest();
            } else {
                response = reader.processPostRequest();
            }

            final Instant endTime = Instant.now();
            completeProcessing(startTime, endTime, logBuffer, method, true);

            return ResponseEntity.ok()
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(response);
                    
        } catch (Exception e) {
            log.error("Exception processing JSON request via " + method, e);
            final Instant endTime = Instant.now();
            completeProcessing(startTime, endTime, logBuffer, method, false);

            return ResponseEntity.badRequest()
                    .contentType(MediaType.APPLICATION_JSON)
                    .body("{\"status\": \"error\", \"message\": \"" + e.getMessage() + "\"}");
        }
    }

    private void completeProcessing(Instant start, Instant end, StringBuffer logBuffer, String method, boolean success) {
        final long duration = java.time.Duration.between(start, end).toMillis();
        
        // Add timing information to log buffer
        logBuffer.append("Request Start time : '").append(Utility.getFormattedDateTime(start.toEpochMilli()))
                .append("' End time : '").append(Utility.getFormattedDateTime(end.toEpochMilli()))
                .append("' Processing time : '").append(duration).append(" milliseconds\n")
                .append("Status : ").append(success ? "SUCCESS" : "FAILED").append("\n")
                .append("\n##########################################\n");

        // Log to TimeTakenInterfaceLog
        TimeTakenInterfaceLog.log("Request Start time : '" + Utility.getFormattedDateTime(start.toEpochMilli()) + 
                "' End time : '" + Utility.getFormattedDateTime(end.toEpochMilli()) + 
                "' Processing time : '" + duration + "' milliseconds");

        // Log to JSONReceiverLog
        JSONReceiverLog.log(logBuffer.toString());

        // Log to application logs
        if (log.isInfoEnabled()) {
            log.info(method + " JSON request processed in " + duration + "ms - " + 
                    (success ? "SUCCESS" : "FAILED"));
        }
    }
}