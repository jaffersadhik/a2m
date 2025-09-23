package com.itextos.beacon.web.generichttpapi.controller;

import java.util.Map;

import com.itextos.beacon.interfaces.generichttpapi.processor.reader.QSRequestReader;
import com.itextos.beacon.interfaces.generichttpapi.processor.reader.RequestReader;

public class ReactiveCustomQSReader {

	
	Map<String, String> params=null;
	String method=null;
	String requestType=null;
	StringBuffer stringBuffer=null;
	
	public ReactiveCustomQSReader(Map<String, String> params, String method, String requestType,
			StringBuffer stringBuffer) {
		this.method=method;
		this.params=params;
		this.requestType=requestType;
		this.stringBuffer=stringBuffer;
	}

	public String processGetRequest() throws Exception {
		
		final RequestReader reader = new QSRequestReader( params,  method,  requestType,stringBuffer);
    
  
        return reader.processGetRequest();

    
	}

}
