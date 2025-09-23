package com.itextos.beacon.platform.topic2table.es;

import java.util.HashMap;
import java.util.Map;

public class ESIndexColMapValue
{

    String  ColumnName;
    String  MappedName;
    String  ColumnType;
    String  DefaultValue;
    boolean CIColumnRequired;

    public ESIndexColMapValue(
            String pColName,
            String pMapName,
            String pColType,
            String pDefault,
            boolean pCIReq)
    {
        this.ColumnName       = pColName;
        this.MappedName       = pMapName;
        this.ColumnType       = pColType;
        this.DefaultValue     = pDefault;
        this.CIColumnRequired = pCIReq;
    }

    public String toString() {
    	
    	Map<String,String> data=new HashMap<String,String>();
    	
    	data.put("ColumnName", ColumnName);
    	
    	data.put("MappedName", MappedName);
    	data.put("ColumnType", ColumnType);
    	data.put("DefaultValue", DefaultValue);
    	data.put("CIColumnRequired",""+ CIColumnRequired);
    	
    	return data.toString();

    }
}
