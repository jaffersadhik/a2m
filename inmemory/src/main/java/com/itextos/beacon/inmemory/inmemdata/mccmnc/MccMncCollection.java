package com.itextos.beacon.inmemory.inmemdata.mccmnc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import com.itextos.beacon.commonlib.utility.CommonUtility;
import com.itextos.beacon.inmemory.loader.process.AbstractAutoRefreshInMemoryProcessor;
import com.itextos.beacon.inmemory.loader.process.InmemoryInput;

public class MccMncCollection
        extends
        AbstractAutoRefreshInMemoryProcessor
{
	
	private static final int BATCH_SIZE = 50;


    private final Map<String, MccMncInfo> mMccMncData = new HashMap<>();

    public MccMncCollection(
            InmemoryInput aInmemoryInputDetail)
    {
        super(aInmemoryInputDetail);
    }

    @Override
    protected void processResultSet(
            ResultSet aResultSet)
            throws SQLException
    {
        long startTime = System.currentTimeMillis();

        int count = 0;

        while (aResultSet.next())
        {
            final MccMncInfo ci = getMccMncInfoFromDB(aResultSet);
            mMccMncData.put(ci.getPrefix(), ci);
            
            count++;
            
            // Process in batches to reduce memory pressure
            if (count % BATCH_SIZE == 0) {
            	
            	if (System.currentTimeMillis() - startTime > 5000) { // 5 second threshold
                    try {
                        Thread.sleep(1000); // 1 second break
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                    startTime = System.currentTimeMillis();
                }
                // Optional: yield thread to prevent CPU monopolization
                Thread.yield();
            }
        }
    }

    private static MccMncInfo getMccMncInfoFromDB(
            ResultSet aResultSet)
            throws SQLException
    {
      
        return new MccMncInfo(CommonUtility.nullCheck(aResultSet.getString("mcc"), true), CommonUtility.nullCheck(aResultSet.getString("mnc"), true),
                CommonUtility.nullCheck(aResultSet.getString("prefix"), true));
    }

    public MccMncInfo getMccMncData(
            String prefix)
    {
        return mMccMncData.get(prefix);
    }

}
