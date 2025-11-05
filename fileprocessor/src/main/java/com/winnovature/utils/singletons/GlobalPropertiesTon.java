package com.winnovature.utils.singletons;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.SystemConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.winnovature.utils.utils.Constants;

public class GlobalPropertiesTon {
	static Log log = LogFactory.getLog(Constants.UtilsLogger);

	private static GlobalPropertiesTon globalProp = new GlobalPropertiesTon();
	private PropertiesConfiguration globalConfiguration;
	private Configuration systemConfiguration;

	final static String GLOBAL_PROPERTY = "global.properties.loc";

	public static GlobalPropertiesTon getInstance() {
		return globalProp;
	}

	private void loadGlobalProperties() throws Exception {
		if (log.isDebugEnabled())
			log.debug("LOADING GLOBAL PROPERTIES....");

		systemConfiguration = new SystemConfiguration();
		String globalPropertiesLocation = (String) systemConfiguration.getProperty(GLOBAL_PROPERTY);
		globalPropertiesLocation="/fileprocessor/global.properties_"+System.getenv("propertyip");
		if (StringUtils.isNotEmpty(globalPropertiesLocation)) {
			if (log.isDebugEnabled())
				log.debug("Global Property File Location - " + globalPropertiesLocation);

			/** Load the properties * */
			 Configurations configs = new Configurations();
			 globalConfiguration = configs.properties(globalPropertiesLocation);
			 
			    if (log.isDebugEnabled())
				log.debug("GLOBAL PROPERTIES LOADED....");
		} else {
			throw new Exception("[GlobalPropertiesTon] COULD NOT LOCATE -Dglobal.properties.loc PARAMETER");
		}
	}

	public PropertiesConfiguration getGlobalConfigObj() throws Exception {
		if (globalConfiguration == null)
			loadGlobalProperties();

		return globalConfiguration;
	}
}
