package com.itextos.beacon.commonlib.encryption.controller;

import java.util.HashMap;
import java.util.Map;

import com.itextos.beacon.commonlib.encryption.process.Processor;
import com.itextos.beacon.commonlib.pwdencryption.EncryptedObject;

public class ReactiveQSRequestReader {

	
	private static final String DESTINATION      = "jsp/encryption.jsp";
	private static final String ENCRYPT          = "encrypt";
	private static final String DECRYPT          = "decrypt";
	private static final String ENCODE           = "encode";
	private static final String DECODE           = "decode";

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

	        
	       return doGet(params);

	      
	}

	
	
	 protected String doGet(
	            Map<String,String> request)
	            
	    {
	        String          lEncodedText     = null;
	        String          lDecodedText     = null;
	        String          lEncryptedText   = null;
	        String          lDecryptedText   = null;
	        String          cryptoType       = null;
	        String          textToEncode     = null;
	        String          textToDecode     = null;
	        String          textToEncrypt    = null;
	        String          textToDecrypt    = null;
	        String          encryptKey       = null;
	        String          decryptKey       = null;
	        EncryptedObject lEncryptedObject = null;

	        try
	        {
	            cryptoType    = request.get("cryptotype");
	            textToEncode  = request.get("encodetext");
	            textToDecode  = request.get("decodetext");
	            textToEncrypt = request.get("encrypttext");
	            textToDecrypt = request.get("decrypttext");
	            encryptKey    = request.get("ekey");
	            decryptKey    = request.get("dkey");

	            if (cryptoType != null)
	                if (cryptoType.equals(ENCRYPT) && (encryptKey != null))
	                {
	                    lEncryptedObject = Processor.encryptProcess("aes256", textToEncrypt, encryptKey);
	                    lEncryptedText   = lEncryptedObject.getEncryptedWithIvAndSalt();
	                }
	                else
	                    if (cryptoType.equals(DECRYPT))
	                        lDecryptedText = Processor.decryptProcess("aes256", textToDecrypt, decryptKey);
	                    else
	                        if (cryptoType.equals(ENCODE))
	                        {
	                            lEncryptedObject = Processor.encodeProcess(ENCODE, textToEncode, null);
	                            lEncodedText     = lEncryptedObject.getEncryptedWithIvAndSalt();
	                        }
	                        else
	                            if (cryptoType.equals(DECODE))
	                                lDecodedText = Processor.decodeProcess(ENCODE, textToDecode, null);
	        }
	        catch (final Exception e)
	        {
	        }
	        finally
	        {
	            final Map<String,String> lSession = new HashMap<String,String>();
	            lSession.put("encodeText", textToEncode);
	            lSession.put("encodedText", lEncodedText);
	            lSession.put("decodeText", textToDecode);
	            lSession.put("decodedText", lDecodedText);
	            lSession.put("enryptedText", lEncryptedText);
	            lSession.put("decryptedText", lDecryptedText);
	            lSession.put("cryptotype", cryptoType);
	            lSession.put("etext", textToEncrypt);
	            lSession.put("dtext", textToDecrypt);
	            lSession.put("ekey", encryptKey);
	            lSession.put("dkey", decryptKey);

	            return lSession.toString();
	        }
	    }
}
