package com.itextos.beacon.platform.pendingpayloadcountexporter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.MetricsServlet;

class Prometheus
{

    private static final Log   log         = LogFactory.getLog(Prometheus.class);
    public static final String METRIC_NAME = "PendingPayloadCount";
    public static final String HELP_INFO   = "PendingPayloadCount";

   
    private static Gauge lCounter;

    static void registerMetrics()
    {
        final String[] labels =
        { "ClusterType", "RedisIndex", "Year", "Month", "Date", "Hour" };

        lCounter = Gauge.build().name(METRIC_NAME).help(HELP_INFO).labelNames(labels).register();
    }

    static void setGaugeValue(
            String aClusterType,
            String aRedisIndex,
            String aYear,
            String aMonth,
            String aDate,
            String aHour,
            long aCount)
    {
        final String[] temp =
        { aClusterType, aRedisIndex, aYear, aMonth, aDate, aHour };
        lCounter.labels(temp).set(aCount);
    }

    static void resetOldValues()
    {
        lCounter.clear();
    }

}