package com.winnovature.initialstate.singletons;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.winnovature.initialstate.utils.Constants;
import com.winnovature.utils.singletons.GlobalPropertiesTon;

public class InitialStagePropertiesTon {

	static Log log = LogFactory.getLog(Constants.InitialStageLogger);

	private static InitialStagePropertiesTon singleton = new InitialStagePropertiesTon();
	private PropertiesConfiguration propConf;
	static final String propertyFileLookupKey = "initial.stage.properties.loc";
	private String className = "InitialStagePropertiesTon";

	private InitialStagePropertiesTon() {
	}

	public static InitialStagePropertiesTon getInstance() {
		return singleton;
	}

	public PropertiesConfiguration getPropertiesConfiguration() throws Exception {
		if (propConf == null)
			loadProperties();

		return propConf;
	}

	private void loadProperties() throws Exception {
		String methodName = " [loadProperties] ";
		if (log.isDebugEnabled())
			log.debug(className + methodName + " LOADING PROPERTIES....");

		PropertiesConfiguration globalProp;
		/** Get the Path for the Properties from the Global Properties * */
		try {
			globalProp = GlobalPropertiesTon.getInstance().getGlobalConfigObj();
		} catch (Exception e) {
			log.error(className + methodName + "Exception: ", e);
			throw new Exception("***** ERROR: GETTING GLOBAL PROPERTIES CONFIGURATION *****");
		}

		try {
			String propertyFilePath = globalProp.getString(propertyFileLookupKey);

			if (StringUtils.isNotBlank(propertyFilePath)) {
				if (log.isDebugEnabled())
					log.debug(className + methodName + " Properties file - " + propertyFilePath);
				 Configurations configs = new Configurations();
				    propConf = configs.properties(propertyFilePath);
			} else {
				throw new Exception("****** ERROR: EITHER IT COULD NOT FIND " + propertyFilePath
						+ " PROPERTY OR HAVE NO VALUES ****");
			}
		} catch (Exception e) {
			log.error(className + methodName + "Exception: ", e);
			throw e;
		}
	}

}
