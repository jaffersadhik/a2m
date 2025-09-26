package com.itextos.beacon.inmemory.templates;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.itextos.beacon.commonlib.utility.CommonUtility;
import com.itextos.beacon.inmemory.loader.process.AbstractAutoRefreshInMemoryProcessor;
import com.itextos.beacon.inmemory.loader.process.InmemoryInput;
import com.itextos.beacon.inmemory.templates.pojo.DLTMsgTemplateObj;

public class DLTMsgTemplates
        extends
        AbstractAutoRefreshInMemoryProcessor
{
	private static final int BATCH_SIZE = 1000;

    private static final Log                     log             = LogFactory.getLog(DLTMsgTemplates.class);

    private Map<String, List<DLTMsgTemplateObj>> mMsgTemplateMap = new HashMap<>();

    public DLTMsgTemplates(
            InmemoryInput aInmemoryInputDetail)
    {
        super(aInmemoryInputDetail);
    }

    public List<DLTMsgTemplateObj> getMsgTemplates(
            String aDltTemplateGroupId,
            String aHeader,
            String aEntityId)
    {
        return mMsgTemplateMap.get(CommonUtility.combine(aDltTemplateGroupId, CommonUtility.nullCheck(aEntityId, true).toLowerCase(), CommonUtility.nullCheck(aHeader, true).toLowerCase()));
    }

    @Override
    protected void processResultSet(
            ResultSet aResultSet)
            throws SQLException
    {
        long startTime = System.currentTimeMillis();

        int count = 0;

        if (log.isDebugEnabled())
            log.debug("Calling the resultset process of " + this.getClass());

        final Map<String, List<DLTMsgTemplateObj>> loadDLTMsgTemplatesMap = new HashMap<>();

        while (aResultSet.next())
        {
            final String lTemplateGroupId = CommonUtility.nullCheck(aResultSet.getString("templ_group_id"), true);
            final String lHeader          = CommonUtility.nullCheck(aResultSet.getString("header"), true);
            final String lTemplateId      = CommonUtility.nullCheck(aResultSet.getString("template_id"), true);
            final String lEntityId        = CommonUtility.nullCheck(aResultSet.getString("entity_id"), true);
            final String lMsgTemplate     = CommonUtility.nullCheck(aResultSet.getString("message_template"), true);
            final String lTemplateType    = CommonUtility.nullCheck(aResultSet.getString("template_type"), true);
            String       lTempMsgTempl    = StringUtils.replace(lMsgTemplate, "{#var#}", ".*");
            lTempMsgTempl = StringUtils.replace(lTempMsgTempl, "\r\n", "\n");

            final DLTMsgTemplateObj       lMsgTemplObj = new DLTMsgTemplateObj(lTemplateId, lEntityId, lTemplateType, lTempMsgTempl);
            final List<DLTMsgTemplateObj> lMsgTemplLst = loadDLTMsgTemplatesMap.computeIfAbsent(CommonUtility.combine(lTemplateGroupId, lEntityId, lHeader), k -> new ArrayList<>());

            lMsgTemplLst.add(lMsgTemplObj);
            
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
            	
                Thread.yield();
            }
        }

        mMsgTemplateMap = loadDLTMsgTemplatesMap;
    }

}
