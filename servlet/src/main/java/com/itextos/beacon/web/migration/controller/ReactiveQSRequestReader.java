package com.itextos.beacon.web.migration.controller;

import java.util.Map;

import com.itextos.beacon.http.interfaceutil.MessageSource;
import com.itextos.beacon.interfaces.generichttpapi.processor.reader.QSRequestReader;
import com.itextos.beacon.interfaces.generichttpapi.processor.reader.RequestReader;
import com.itextos.beacon.interfaces.migration.processor.reader.MJsonRequestReader;
import com.itextos.beacon.interfaces.migration.processor.reader.MRequestReader;

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

		
		   final MRequestReader lMRequestReader = new MJsonRequestReader(params, MessageSource.GENERIC_QS, MessageSource.GENERIC_QS,new StringBuffer());

	        
	       return lMRequestReader.processGetRequest();

	      
	}

}
