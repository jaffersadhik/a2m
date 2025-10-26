package com.itextos.beacon.platform.dnr.controller;

import java.util.Map;

import com.itextos.beacon.platform.dnr.process.DlrProcess;

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

	        
	       return DlrProcess.doProcess(params);

	      
	}

}
