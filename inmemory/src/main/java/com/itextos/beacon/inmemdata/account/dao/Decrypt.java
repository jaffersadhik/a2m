package com.itextos.beacon.inmemdata.account.dao;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.itextos.beacon.commonlib.constants.exception.ItextosException;
import com.itextos.beacon.commonlib.pwdencryption.Encryptor;
import com.itextos.beacon.commonlib.utility.CommonUtility;
import com.itextos.beacon.inmemdata.account.UserInfo;

public class Decrypt implements Runnable{

    private static final Log      log                     = LogFactory.getLog(Decrypt.class);

	 private Map<String, UserInfo> userPassMap  ;
	 private Map<String, UserInfo> accessKeyMap ;
	 private Map<String, UserInfo> clientIdMap  ;
	 private UserInfo userinfo ;
	 private boolean firstTime;
	public Decrypt(Map<String, UserInfo> userPassMap,Map<String, UserInfo> accessKeyMap,Map<String, UserInfo> clientIdMap,UserInfo userinfo,boolean firstTime) {
		this.accessKeyMap=accessKeyMap;
		this.userPassMap=userPassMap;
		this.clientIdMap=clientIdMap;
		this.userinfo=userinfo;
		this.firstTime=firstTime;
	}
	public void run() {
		
		 doProcess( userPassMap, accessKeyMap, clientIdMap, userinfo);

  
	}
	
	
	public void gotosleep() {
		
		if(!firstTime) {

    	try {
			Thread.sleep(250L);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		}
	}
	
	private void doProcess(Map<String, UserInfo> userPassMap,Map<String, UserInfo> accessKeyMap,Map<String, UserInfo> clientIdMap,UserInfo userinfo) {
		try
        {
            final String clientId          = userinfo.getClientId();

            final String lApiPass          = userinfo.getApiPassword();
            final String lSmppPass         = userinfo.getSmppPassword();

            final String decryptedApiPass  = decryptApiPassword(lApiPass, clientId);
            gotosleep();
            final String decryptedSmppPass = decryptSmppPassword(lSmppPass, clientId);
            gotosleep();
            final String cliId             = CommonUtility.nullCheck(userinfo.getClientId(), true);
            final String userName          = CommonUtility.nullCheck(userinfo.getUserName(), true).toLowerCase();

            if (cliId.isBlank())
                throw new ItextosException("Invalid client Id specified.");

            if (userName.isBlank())
                throw new ItextosException("Invalid username specified.");

            final UserInfo userInfo = new UserInfo(cliId, userName, decryptedApiPass, decryptedSmppPass, userinfo.getStatus());

            userPassMap.put(userInfo.getUserName(), userInfo);
            clientIdMap.put(userInfo.getClientId(), userInfo);

            if (userInfo.getApiPassword()!=null&&!userInfo.getApiPassword().isBlank())
           	 accessKeyMap.put(userInfo.getApiPassword(), userInfo);
        }
        catch (final Exception e)
        {
        }
		
	}
	private static String decryptApiPassword(
            String aApiPass,
            String aClientId)
    {
        String returnValue = null;

        try
        {
            returnValue = aApiPass.isBlank() ? "" : Encryptor.getApiDecryptedPassword(aApiPass);
        }
        catch (final Exception e)
        {
            returnValue = "";
            if (log.isDebugEnabled()) // Specifically used DEBUG
                log.error("Exception while decrypting user API password from database. Client id : '" + aClientId + "'", e);
        }
        return returnValue;
    }

    private static String decryptSmppPassword(
            String aSmppPass,
            String aClientId)
    {
        String returnValue = null;

        try
        {
            returnValue = aSmppPass.isBlank() ? "" : Encryptor.getSmppDecryptedPassword(aSmppPass);
        }
        catch (final Exception e)
        {
            returnValue = "";
            if (log.isDebugEnabled()) // Specifically used DEBUG
                log.error("Exception while decrypting user SMPP password from database. Client id : '" + aClientId + "'", e);
        }
        return returnValue;
    }

}
