package com.itextos.beacon.commonlib.password.controller;

import java.util.Map;

import com.itextos.beacon.commonlib.pwdencryption.EncryptedObject;
import com.itextos.beacon.commonlib.pwdencryption.Encryptor;
import com.itextos.beacon.commonlib.utility.CommonUtility;
import com.itextos.beacon.interfaces.generichttpapi.processor.reader.QSRequestReader;
import com.itextos.beacon.interfaces.generichttpapi.processor.reader.RequestReader;

import jakarta.servlet.http.HttpServletResponse;

public class ReactiveQSRequestReader {

	
	private static final String PARAM_CLIENT_ID  = "cli_id";
	private static final String PARAM_API_KEY    = "reset_api";
	private static final String PARAM_SMPP_KEY   = "reset_smpp";
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
	        
	       return doGet(params);

	      
	}

	
	
	private String doGet(
            Map<String,String> request
            )
            
    {
        final long   startTime      = System.currentTimeMillis();
        String       clientId       = request.get(PARAM_CLIENT_ID);
        String       apiRequest     = request.get(PARAM_API_KEY);
        String       smppRequest    = request.get(PARAM_SMPP_KEY);
//        final String ip             = request.getRemoteAddr();
        boolean      status         = false;
        int          httpStatus     = HttpServletResponse.SC_ACCEPTED;
        String       responseString = "";

        try
        {
      
            clientId    = CommonUtility.nullCheck(clientId, true);
            apiRequest  = CommonUtility.nullCheck(apiRequest, true);
            smppRequest = CommonUtility.nullCheck(smppRequest, true);

            // if (true)
            // throw new Exception("For testing purpose");

            if (clientId.isBlank())
            {
                responseString = ("Invalid Client Id Specified.");
                httpStatus     = HttpServletResponse.SC_BAD_REQUEST;
                return httpStatus+"";
            }

            if (!CommonUtility.isEnabled(apiRequest) && !CommonUtility.isEnabled(smppRequest))
            {
                responseString = ("Invalid reset option specified. Api Request : '" + apiRequest + "' Smpp Request : '" + smppRequest + "'");
                httpStatus     = HttpServletResponse.SC_BAD_REQUEST;
                return httpStatus+"";
            }

            EncryptedObject apiObject  = null;
            EncryptedObject smppObject = null;

            if (CommonUtility.isEnabled(apiRequest))
                apiObject = Encryptor.getApiPassword();

            if (CommonUtility.isEnabled(smppRequest))
                smppObject = Encryptor.getSmppPassword();

            final String        apiResponse  = getResponse("api", apiObject);
            final String        smppResponse = getResponse("smpp", smppObject);

            final StringBuilder sb           = new StringBuilder();

            sb.append("\"passwords\":[");
            if (!apiResponse.isEmpty())
                sb.append(apiResponse);

            if (CommonUtility.isEnabled(apiRequest) && CommonUtility.isEnabled(smppRequest))
                sb.append(",");

            if (!smppResponse.isEmpty())
                sb.append(smppResponse);

            sb.append("]");
            responseString = sb.toString();

       

            httpStatus = HttpServletResponse.SC_OK;
            status     = true;
        }
        catch (final Exception e)
        {
            httpStatus     = HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
            responseString = "Some Error has happened. " + e.getMessage() + ". Check logs for more details";
        }
        finally
        {
            final long          endTime = System.currentTimeMillis();

            final StringBuilder sb      = new StringBuilder("{");

            if (!status)
            {
                sb.append("\"status\":\"failed\",");
                sb.append("\"timetaken\":\"").append(endTime - startTime).append("\",");
                sb.append("\"reason\":\"").append(responseString).append("\"");
            }
            else
            {
                sb.append("\"status\":\"success\",");
                sb.append("\"timetaken\":\"").append(endTime - startTime).append("\",");
                sb.append("\"reason\":\"\",");
                sb.append(responseString);
            }
            sb.append("}");


           return sb.toString();

              }
    }

    private static String getResponse(
            String aString,
            EncryptedObject aEncryptedObject)
    {
        if (aEncryptedObject == null)
            return "";

        final StringBuilder sb = new StringBuilder("{");
        sb.append("\"type\":\"").append(aString).append("\",");
        sb.append("\"customer_password\":\"").append(aEncryptedObject.getActualString()).append("\",");
        sb.append("\"dbinsert_password\":\"").append(aEncryptedObject.getEncryptedWithIvAndSalt()).append("\"");
        sb.append("}");
        return sb.toString();
    }
}
