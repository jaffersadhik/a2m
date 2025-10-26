package com.itextos.beacon.web.generichttpapi.controller;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.itextos.beacon.commonlib.constants.MiddlewareConstant;
import com.itextos.beacon.commonlib.utility.ClientIP;
import com.itextos.beacon.http.generichttpapi.common.utils.InterfaceInputParameters;
import com.itextos.beacon.http.generichttpapi.common.utils.Utility;
import com.itextos.beacon.smslog.TimeTakenInterfaceLog;

import java.time.Instant;
import java.util.Map;

import jakarta.servlet.http.HttpServletRequest;

@RestController
@RequestMapping("/genericapi/QSCustomReceiver")
public class QSCustomReceiverController {

    private static final Log log = LogFactory.getLog(QSCustomReceiverController.class);

    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> handleCustomQSGet(
            @RequestParam Map<String, String> allParams,
            @RequestHeader(value = "X-Forwarded-For", required = false) String clientIp,
            @RequestHeader(value = "Authorization", required = false) String authorization,
            HttpServletRequest request) {
        
        return processCustomQSRequest("GET", allParams, clientIp, authorization, request, null);
    }

    @PostMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> handleCustomQSPost(
            @RequestBody(required = false) String requestBody,
            @RequestParam Map<String, String> allParams,
            @RequestHeader(value = "X-Forwarded-For", required = false) String clientIp,
            @RequestHeader(value = "Authorization", required = false) String authorization,
            HttpServletRequest request) {
        
        return processCustomQSRequest("POST", allParams, clientIp, authorization, request, requestBody);
    }

    private ResponseEntity<String> processCustomQSRequest(String method, Map<String, String> params, String clientIp, 
                                                         String authorization, HttpServletRequest request, 
                                                         String requestBody) {
        final Instant startTime = Instant.now();
        
        if (log.isDebugEnabled()) {
            log.debug("Custom QS request received via " + method);
        }

        try {
            params.put(MiddlewareConstant.MW_CLIENT_SOURCE_IP.getKey(), ClientIP.getClientIpAddress(clientIp, request));
            params.put(InterfaceInputParameters.AUTHORIZATION, authorization);

            // Process request synchronously
            StringBuffer sb = new StringBuffer();
            CustomQSReader reader = new CustomQSReader(params, method, "custom", sb);
            
            String response = reader.processGetRequest();

            final Instant endTime = Instant.now();
            logProcessingTime(startTime, endTime, method);

            return ResponseEntity.ok()
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(response);
                    
        } catch (Exception e) {
            log.error("Exception processing custom QS request", e);
            final Instant endTime = Instant.now();
            logProcessingTime(startTime, endTime, method);

            return ResponseEntity.badRequest()
                    .contentType(MediaType.APPLICATION_JSON)
                    .body("{\"status\": \"error\", \"message\": \"Processing failed: " + e.getMessage() + "\"}");
        }
    }

    private void logProcessingTime(Instant start, Instant end, String method) {
        final long duration = java.time.Duration.between(start, end).toMillis();
        
        // Log to TimeTakenInterfaceLog
        TimeTakenInterfaceLog.log("Request Start time : '" + Utility.getFormattedDateTime(start.toEpochMilli()) + 
                "' End time : '" + Utility.getFormattedDateTime(end.toEpochMilli()) + 
                "' Processing time : '" + duration + "' milliseconds");

        // Log to application logs
        if (log.isInfoEnabled()) {
            log.info(method + " Request Start time: '" + Utility.getFormattedDateTime(start.toEpochMilli()) + 
                    "' End time: '" + Utility.getFormattedDateTime(end.toEpochMilli()) + 
                    "' Processing time: '" + duration + "' milliseconds");
        }
    }

    
}