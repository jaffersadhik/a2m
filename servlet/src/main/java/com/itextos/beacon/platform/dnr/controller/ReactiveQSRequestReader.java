package com.itextos.beacon.platform.dnr.controller;

import java.util.Map;

import com.itextos.beacon.interfaces.generichttpapi.processor.reader.QSRequestReader;
import com.itextos.beacon.interfaces.generichttpapi.processor.reader.RequestReader;
import com.itextos.beacon.platform.dnr.process.DlrProcess;
import com.itextos.beacon.platform.msgtool.util.MsgProcessUtil;

public class ReactiveQSRequestReader {

	Map<String, String> params=null;
	String method=null;
	String requestType=null;
	StringBuffer stringBuffer=null;
	public ReactiveQSRequestReader(Map<String, String> params, String method, String requestType,
			StringBuffer stringBuffer) {
		
		this.method=method;
		this.params=params;
		this.requestType=requestType;
		this.stringBuffer=stringBuffer;
				
	}

	public String processRequest() throws Exception {

		 final RequestReader reader = new QSRequestReader( params,  method,  requestType,stringBuffer);
	        
	       return DlrProcess.doProcess(params);

	      
	}

}
