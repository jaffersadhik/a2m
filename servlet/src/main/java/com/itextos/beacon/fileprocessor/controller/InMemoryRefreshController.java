package com.itextos.beacon.fileprocessor.controller;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.winnovature.utils.singletons.ConfigParamsTon;

import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/fileprocessor/memoryrefresh")
public class InMemoryRefreshController {

    private static final Log log = LogFactory.getLog("MemoryRefreshLogger");
    private static final String className = "InMemoryRefreshController";

    @GetMapping(produces = MediaType.TEXT_HTML_VALUE)
    public Mono<String> handleGet(@RequestParam(value = "reqtype", required = false) String reqtype) {
        String logName = className + " [handleGet] ";

        if (log.isDebugEnabled()) {
            log.debug(logName + "begin, reqtype = " + reqtype);
        }

        if (StringUtils.isBlank(reqtype)) {
            return displayMenu();
        }

        reqtype = reqtype.trim();
        if (reqtype.equalsIgnoreCase("CP")) {
            try {
                ConfigParamsTon.getInstance().reload();
                return Mono.just("<font color='green'>Config params reloaded</font>");
            } catch (Exception e) {
                log.error(logName + "Exception: ", e);
                return Mono.just("<font color='red'>Exception while reloading Config Params table.</font>");
            }
        } else {
            return displayMenu();
        }
    }

    @PostMapping(path = "/memoryrefresh", produces = MediaType.TEXT_HTML_VALUE)
    public Mono<String> handlePost(@RequestParam(value = "reqtype", required = false) String reqtype) {
        return handleGet(reqtype);
    }

    private Mono<String> displayMenu() {
        StringBuilder menu = new StringBuilder();
        menu.append("<B>http://IP:PORT/FP-InMemoryRefresh-0.0.1/memoryrefresh?reqtype={here use any one of below keys....}</B>");
        menu.append("<br><ul>");
        menu.append("<li>CP - Config Param &nbsp;&nbsp;(TABLE NAME = CONFIG_PARAMS)</li></ul>");
        menu.append("</ul>");
        
        return Mono.just(menu.toString());
    }
}