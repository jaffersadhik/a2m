package com.itextos.beacon.inmemory.msgutil.cache;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.itextos.beacon.errorlog.MemoryLoaderLog;
import com.itextos.beacon.inmemory.loader.process.AbstractAutoRefreshInMemoryProcessor;
import com.itextos.beacon.inmemory.loader.process.InmemoryInput;

public class CarrierCircles
        extends
        AbstractAutoRefreshInMemoryProcessor
{
	private static final int BATCH_SIZE = 50;

    private static final Log                        log               = LogFactory.getLog(CarrierCircles.class);
    private Map<String, Map<String, CarrierCircle>> mCarrierCircleMap = new HashMap<>();

    public CarrierCircles(
            InmemoryInput aInmemoryInputDetail)
    {
        super(aInmemoryInputDetail);
    }

    public CarrierCircle getCarrierCircle(
            String aMobileNumber)
    {
        return getCarrierCircle(aMobileNumber, false);
    }

    public CarrierCircle getCarrierCircle(
            String aMobileNumber,
            boolean aReturnDefault)
    {
        final String                     prefix = aMobileNumber.substring(0, 4);
        final Map<String, CarrierCircle> inner  = mCarrierCircleMap.get(prefix);

        if (inner == null)
        {
            if (aReturnDefault)
                return CarrierCircle.DEFAULT_CARRIER_CIRCLE;
            return null;
        }

        final CarrierCircle lCarrierCircle = inner.get(aMobileNumber);

        if (lCarrierCircle == null)
        {
            if (aReturnDefault)
                return CarrierCircle.DEFAULT_CARRIER_CIRCLE;
            return null;
        }

        return lCarrierCircle;
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

        // select SUBSTR(msc,1,4) prefix, msc, carrier, circle from
        // configuration.msc_code_map

        final Map<String, Map<String, CarrierCircle>> lTempMscCodes = new HashMap<>();

        while (aResultSet.next())
        {
            final String                     lPrefix  = aResultSet.getString("prefix");
            final String                     lMsc     = aResultSet.getString("msc");
            final String                     lCarrier = aResultSet.getString("carrier");
            final String                     lCircle  = aResultSet.getString("circle");

            final Map<String, CarrierCircle> inner    = lTempMscCodes.computeIfAbsent(lPrefix, k -> new HashMap<>());
            inner.put(lMsc, new CarrierCircle(lCarrier, lCircle));
            

            count++;
            
            // Process in batches to reduce memory pressure
            if (count % BATCH_SIZE == 0) {
            	
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

                // Optional: yield thread to prevent CPU monopolization
                Thread.yield();
            }
        }

        if (!lTempMscCodes.isEmpty())
            mCarrierCircleMap = lTempMscCodes;
    }

}