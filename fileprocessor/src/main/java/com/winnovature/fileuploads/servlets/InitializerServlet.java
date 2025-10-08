package com.winnovature.fileuploads.servlets;

import com.itextos.beacon.commonlib.prometheusmetricsutil.PrometheusMetrics;

public class InitializerServlet  {
	private static final long serialVersionUID = 1L;
	private static final String className = "[InitializeExcludeConsumer]";
	

	
	public void init()  {
		


			try {
				
		        PrometheusMetrics.registerApiMetrics();
			} catch (Exception e) {
			}

		
	}

}
