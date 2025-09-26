package com.itextos.beacon.inmemory.inmemdata.country;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.itextos.beacon.commonlib.utility.CommonUtility;
import com.itextos.beacon.inmemory.loader.process.AbstractAutoRefreshInMemoryProcessor;
import com.itextos.beacon.inmemory.loader.process.InmemoryInput;

public class CountryInfoCollection
        extends AbstractAutoRefreshInMemoryProcessor
{

    // Use ConcurrentHashMap for thread-safe reads without blocking
    private final Map<String, CountryInfo> mCountryData = new ConcurrentHashMap<>();
    
    // Cache for frequently accessed country codes
    private final Map<String, CountryInfo> hotCountryCache = new ConcurrentHashMap<>();
    private static final int HOT_CACHE_SIZE = 50;

    public CountryInfoCollection(InmemoryInput aInmemoryInputDetail) {
        super(aInmemoryInputDetail);
    }

    @Override
    protected void processResultSet(ResultSet aResultSet) throws SQLException {
        // Temporary map to avoid partial state during refresh
        Map<String, CountryInfo> newData = new HashMap<>();
        
        while (aResultSet.next()) {
            final CountryInfo ci = getCountryInfoFromDB(aResultSet);
            newData.put(ci.getCountryCode(), ci);
        }
        
        // Atomic swap of the entire map
   //     mCountryData.clear();
        mCountryData.putAll(newData);
        hotCountryCache.clear(); // Clear hot cache on refresh
    }

    private static CountryInfo getCountryInfoFromDB(ResultSet aResultSet) throws SQLException {
        // Pre-calculate values to avoid repeated method calls
        String otherMobileLength = aResultSet.getString("other_mobile_length");
        String countryCode = CommonUtility.nullCheck(aResultSet.getString("country_code_iso_3"), true);
        String country = CommonUtility.nullCheck(aResultSet.getString("country"), true);
        String shortName = CommonUtility.nullCheck(aResultSet.getString("country_short_name"), true);
        String iso2Code = CommonUtility.nullCheck(aResultSet.getString("country_code_iso_2"), true);
        Integer isoNumeric = CommonUtility.getInteger(aResultSet.getString("country_code_iso_numeric"));
        Integer dialInCode = CommonUtility.getInteger(aResultSet.getString("dial_in_code"));
        String dialInCodeFull = CommonUtility.nullCheck(aResultSet.getString("dial_in_code_full"), true);
        Integer defaultMobileLength = CommonUtility.getInteger(aResultSet.getString("default_mobile_length"));
        Integer minMobileLength = CommonUtility.getInteger(aResultSet.getString("min_mobile_length"));
        Integer maxMobileLength = CommonUtility.getInteger(aResultSet.getString("max_mobile_length"));
        String currency = CommonUtility.nullCheck(aResultSet.getString("country_currency"), true);
        
        int[] otherLengths = parseOtherMobileLengths(otherMobileLength);
        
        return new CountryInfo(countryCode, country, shortName, iso2Code, isoNumeric, 
                dialInCode, dialInCodeFull, defaultMobileLength, otherLengths,
                minMobileLength, maxMobileLength, currency);
    }

    // Optimized parsing method
    private static int[] parseOtherMobileLengths(String otherMobileLength) {
        if (otherMobileLength == null || otherMobileLength.trim().isEmpty()) {
            return null;
        }
        
        String[] lSplit = CommonUtility.split(otherMobileLength, ",");
        if (lSplit == null || lSplit.length == 0) {
            return null;
        }
        
        int[] otherLengths = new int[lSplit.length];
        for (int i = 0; i < lSplit.length; i++) {
            try {
                otherLengths[i] = Integer.parseInt(lSplit[i].trim());
            } catch (NumberFormatException e) {
                otherLengths[i] = 0; // Default value on error
            }
        }
        return otherLengths;
    }

    public CountryInfo getCountryData(String aCountryCode) {
        if (aCountryCode == null) return null;
        
        // Check hot cache first (L1 cache)
        CountryInfo result = hotCountryCache.get(aCountryCode);
        if (result != null) {
            return result;
        }
        
        // Check main data store
        result = mCountryData.get(aCountryCode);
        
        // If found and hot cache isn't full, add to hot cache
        if (result != null && hotCountryCache.size() < HOT_CACHE_SIZE) {
            hotCountryCache.put(aCountryCode, result);
        }
        
        return result;
    }

    // Batch retrieval for multiple country codes
    public Map<String, CountryInfo> getMultipleCountryData(Iterable<String> countryCodes) {
        Map<String, CountryInfo> result = new HashMap<>();
        for (String code : countryCodes) {
            CountryInfo info = getCountryData(code);
            if (info != null) {
                result.put(code, info);
            }
        }
        return result;
    }

    // Size method for monitoring
    public int getCountryCount() {
        return mCountryData.size();
    }

    // Clear hot cache (useful when memory pressure is high)
    public void clearHotCache() {
        hotCountryCache.clear();
    }
}