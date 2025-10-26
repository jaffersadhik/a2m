package com.itextos.beacon.web.generichttpapi.controller;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/genericapi/health")
public class HealthCheckController {

    private static final Log log = LogFactory.getLog(HealthCheckController.class);

    @GetMapping(produces = MediaType.TEXT_PLAIN_VALUE)
    public String healthCheckGet() {
        if (log.isDebugEnabled()) {
            log.debug("Health check request received via GET");
        }
        
        return "ok";
    }

    @PostMapping(produces = MediaType.TEXT_PLAIN_VALUE)
    public String healthCheckPost() {
        if (log.isDebugEnabled()) {
            log.debug("Health check request received via POST");
        }
        
        return "ok";
    }

    // Additional HTTP methods if needed
    @RequestMapping(method = {RequestMethod.PUT, RequestMethod.HEAD, RequestMethod.OPTIONS}, 
                    produces = MediaType.TEXT_PLAIN_VALUE)
    public String healthCheckOtherMethods() {
        return "ok";
    }
}