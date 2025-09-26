package com.itextos.beacon.web.generichttpapi.controller;

import java.util.HashMap;
import java.util.Map;

import com.itextos.beacon.http.interfaceutil.MessageSource;
import com.itextos.beacon.interfaces.generichttpapi.processor.reader.QSRequestReader;
import com.itextos.beacon.interfaces.generichttpapi.processor.reader.RequestReader;
import com.itextos.beacon.smslog.QSReceiverLog;

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

        QSReceiverLog.log("params : "+params);
        QSReceiverLog.log("method : "+method);
        QSReceiverLog.log("requestType : "+requestType);

		 final RequestReader reader = new QSRequestReader( params,  method,  requestType,stringBuffer);
	        
	       return reader.processGetRequest();
	      
	}
	
	public static void initSMS() throws Exception
	{
		
		String method="GET";
		String requestType="gen_qs";
		Map<String, String> params=new HashMap<String, String>();
		
		params.put("version", "1.0");
		params.put("header", "APHSPL");

		params.put("type", "PM");

		params.put("accesskey", "TQC4UUaDDBi2");
		params.put("dest", "919487660738");
		params.put("msg", "Namaste!\r\n"
				+ "				Your Apollo Hospitals verification code is 721303. Submit details securely.");
		params.put("cli_ip", "10.122.0.12");

	    ReactiveQSRequestReader reactiveReader = new ReactiveQSRequestReader(
                params, method, requestType, new StringBuffer());
	    
	    reactiveReader.processRequest();
			
	}
}
