package com.itextos.beacon.inmemory.inmemdata.mccmnc;

import com.itextos.beacon.errorlog.SMSLog;
import com.itextos.beacon.inmemory.loader.InmemoryLoaderCollection;
import com.itextos.beacon.inmemory.loader.process.InmemoryId;

public class MCCMNCFinder
{

    private MCCMNCFinder()
    {}

 

    public static MccMncInfo getMccMnc(
            String aMNumberSeries)
    {
        final MccMncCollection lMccMncCollection = (MccMncCollection) InmemoryLoaderCollection.getInstance().getInmemoryCollection(InmemoryId.MCC_MNC);
        
        if(lMccMncCollection==null) {
        	
        	SMSLog.getInstance().log("lMccMncCollection is null");
        }
        return lMccMncCollection==null?null:lMccMncCollection.getMccMncData(aMNumberSeries);
    }

  

}