package com.itextos.beacon.platform.msgtool.controller;

import java.util.Map;

import com.itextos.beacon.platform.msgtool.util.MsgProcessUtil;

public class QSRequestReader {

	Map<String, String> params=null;
	String method=null;
	String requestType=null;
	StringBuffer stringBuffer=null;
	public QSRequestReader(Map<String, String> params, String method, String requestType,
			StringBuffer stringBuffer) {
		
		this.method=method;
		this.params=params;
		this.requestType=requestType;
		this.stringBuffer=stringBuffer;
				
	}

	public String processRequest() throws Exception {

	        
	       return MsgProcessUtil.requestProcess(params);
	      
	}

}
