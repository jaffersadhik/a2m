package com.itextos.beacon.inmemory.templates;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.itextos.beacon.commonlib.utility.CommonUtility;
import com.itextos.beacon.errorlog.MemoryLoaderLog;
import com.itextos.beacon.inmemory.loader.process.AbstractAutoRefreshInMemoryProcessor;
import com.itextos.beacon.inmemory.loader.process.InmemoryInput;
import com.itextos.beacon.inmemory.templates.pojo.DLTMsgPrefixSuffixObj;

public class DLTMsgTemplatePrefixSuffix
        extends
        AbstractAutoRefreshInMemoryProcessor
{
	
	private static final int BATCH_SIZE = 50;


    private static final Log                   log                         = LogFactory.getLog(DLTMsgTemplatePrefixSuffix.class);

    private Map<String, DLTMsgPrefixSuffixObj> mMsgTemplatePrefixSuffixMap = new HashMap<>();

    public DLTMsgTemplatePrefixSuffix(
            InmemoryInput aInmemoryInputDetail)
    {
        super(aInmemoryInputDetail);
    }

    public DLTMsgPrefixSuffixObj getMsgPrefixSuffixVal(
            String aDltTemplateType)
    {
        return mMsgTemplatePrefixSuffixMap.get(aDltTemplateType);
    }

    @Override
    protected void processResultSet(
            ResultSet aResultSet)
            throws SQLException
    {
    	
        int count = 0;

        long startTime = System.currentTimeMillis();

        if (log.isDebugEnabled())
            log.debug("Calling the resultset process of " + this.getClass());

        final Map<String, DLTMsgPrefixSuffixObj> lDltMsgSuffixPrefixMap = new HashMap<>();

        while (aResultSet.next())
        {
            final String                lTemplateType       = CommonUtility.nullCheck(aResultSet.getString("template_type"), true).toLowerCase();
            final String                lMsgPrefix          = CommonUtility.nullCheck(aResultSet.getString("msg_prefix"), false);
            final String                lMsgSuffix          = CommonUtility.nullCheck(aResultSet.getString("msg_suffix"), false);
            final DLTMsgPrefixSuffixObj lMsgPrefixSuffixObj = new DLTMsgPrefixSuffixObj(lMsgPrefix, lMsgSuffix);
            lDltMsgSuffixPrefixMap.put(lTemplateType, lMsgPrefixSuffixObj);
            
            count++;
            
            // Process in batches to reduce memory pressure
            if (count % BATCH_SIZE == 0) {
                // Optional: yield thread to prevent CPU monopolization
            	
            	if (System.currentTimeMillis() - startTime > 5000) { // 5 second threshold
                    log.warn("Processing too slow, yielding to prevent CPU spike");
                    try {
                        Thread.sleep(1000); // 1 second break
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                    startTime = System.currentTimeMillis();
                }
            	MemoryLoaderLog.log(this.getClass().getName());

                Thread.yield();
            }
        }

        if (!lDltMsgSuffixPrefixMap.isEmpty())
            mMsgTemplatePrefixSuffixMap = lDltMsgSuffixPrefixMap;
    }

}