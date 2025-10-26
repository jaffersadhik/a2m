package com.itextos.beacon.commonlib.utility;

import org.springframework.util.StringUtils;

import javax.servlet.http.HttpServletRequest;

public class ClientIP {

    public static String getClientIpAddress(String xForwardedFor, HttpServletRequest request) {
        // Priority 1: Use X-Forwarded-For header if present
        if (StringUtils.hasText(xForwardedFor)) {
            String[] ips = xForwardedFor.split(",");
            return ips[0].trim();
        }
        
        // Priority 2: Get remote address from request
        if (request != null) {
            String remoteAddr = request.getRemoteAddr();
            if (StringUtils.hasText(remoteAddr) && !"unknown".equalsIgnoreCase(remoteAddr)) {
                return remoteAddr;
            }
        }
        
        return "unknown";
    }

    // Alternative method that only requires HttpServletRequest
    public static String getClientIpAddress(HttpServletRequest request) {
        if (request == null) {
            return "unknown";
        }

        // Check X-Forwarded-For header first
        String xForwardedFor = request.getHeader("X-Forwarded-For");
        if (StringUtils.hasText(xForwardedFor)) {
            String[] ips = xForwardedFor.split(",");
            return ips[0].trim();
        }

        // Check other common proxy headers
        String ip = request.getHeader("X-Real-IP");
        if (StringUtils.hasText(ip) && !"unknown".equalsIgnoreCase(ip)) {
            return ip;
        }

        ip = request.getHeader("Proxy-Client-IP");
        if (StringUtils.hasText(ip) && !"unknown".equalsIgnoreCase(ip)) {
            return ip;
        }

        ip = request.getHeader("WL-Proxy-Client-IP");
        if (StringUtils.hasText(ip) && !"unknown".equalsIgnoreCase(ip)) {
            return ip;
        }

        ip = request.getHeader("HTTP_CLIENT_IP");
        if (StringUtils.hasText(ip) && !"unknown".equalsIgnoreCase(ip)) {
            return ip;
        }

        ip = request.getHeader("HTTP_X_FORWARDED_FOR");
        if (StringUtils.hasText(ip) && !"unknown".equalsIgnoreCase(ip)) {
            return ip;
        }

        // Fall back to remote address
        return request.getRemoteAddr();
    }
}