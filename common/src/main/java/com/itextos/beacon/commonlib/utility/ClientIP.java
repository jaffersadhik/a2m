package com.itextos.beacon.commonlib.utility;

import java.net.InetSocketAddress;

import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.util.StringUtils;

public class ClientIP {

	public static String getClientIpAddress(String xForwardedFor, ServerHttpRequest request) {
	    // Priority 1: Use X-Forwarded-For header if present
	    if (StringUtils.hasText(xForwardedFor)) {
	        String[] ips = xForwardedFor.split(",");
	        return ips[0].trim();
	    }
	    
	    // Priority 2: Get remote address from request
	    InetSocketAddress remoteAddress = request.getRemoteAddress();
	    if (remoteAddress != null && remoteAddress.getAddress() != null) {
	        return remoteAddress.getAddress().getHostAddress();
	    }
	    
	    return "unknown";
	}
}
