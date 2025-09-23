package com.itextos.beacon.interfaces.migration.processor.reader;

import org.json.simple.JSONObject;

import com.itextos.beacon.http.generichttpapi.common.interfaces.IRequestProcessor;

public interface MRequestReader
{

	String processGetRequest();

    String processPostRequest()
            throws Exception;

    String doProcess();

    String doProcess(
            JSONObject aJsonObj);

    String sendResponse(
            IRequestProcessor aRequestProcessor);

   

}
