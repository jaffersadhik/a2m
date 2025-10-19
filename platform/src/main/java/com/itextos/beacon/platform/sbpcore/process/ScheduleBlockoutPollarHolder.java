package com.itextos.beacon.platform.sbpcore.process;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.itextos.beacon.commonlib.constants.TimerIntervalConstant;
import com.itextos.beacon.commonlib.utility.CommonUtility;
import com.itextos.beacon.commonlib.utility.timer.ITimedProcess;
import com.itextos.beacon.commonlib.utility.timer.TimedProcessor;
import com.itextos.beacon.commonlib.utility.tp.ExecutorShedulePoller;
import com.itextos.beacon.platform.sbc.util.ConsumerId;
import com.itextos.beacon.platform.sbpcore.dao.DBPoller;
import com.itextos.beacon.smslog.DebugLog;
import com.itextos.beacon.smslog.SchedulePollerLog;

public class ScheduleBlockoutPollarHolder
        implements
        ITimedProcess
{

    private static final Log log = LogFactory.getLog(ScheduleBlockoutPollarHolder.class);

    private static class SingletonHolder
    {

        @SuppressWarnings("synthetic-access")
        static final ScheduleBlockoutPollarHolder INSTANCE = new ScheduleBlockoutPollarHolder();

    }

    public static ScheduleBlockoutPollarHolder getInstance()
    {
        return SingletonHolder.INSTANCE;
    }

    private static final String                                 SCHEDULE    = "schedule";
    private static final String                                 BLOCOUT     = "blockout";
    private final Map<String, Map<Integer, AbstractDataPoller>> mPollers    = new HashMap<>();
    private boolean                                             canContinue = true;

    private ScheduleBlockoutPollarHolder()
    {
        startPollars();
       
     }

    private void startPollars()
    {

      
            final List<String> aInstanceIds = ConsumerId.getInstance().getConsumerList();

            for (final String lInstanceId : aInstanceIds)
            {
                final Integer instanceId = CommonUtility.getInteger(lInstanceId);
                addToList(SCHEDULE, instanceId, new SchedulePoller(instanceId));
                addToList(BLOCOUT, instanceId, new BlockoutPoller(instanceId));
            }
       
       
    }

  

    
   

    private void addToList(
            String aType,
            Integer aInstanceId,
            AbstractDataPoller aAbstractPoller)
    {
        final Map<Integer, AbstractDataPoller> pollerList = mPollers.computeIfAbsent(aType, k -> new HashMap<>());
        pollerList.put(aInstanceId, aAbstractPoller);
        log.info("Poller started for " + aType + " and instance id " + aInstanceId);
    }

    @Override
    public void stopMe()
    {

        for (final Entry<String, Map<Integer, AbstractDataPoller>> entry : mPollers.entrySet())
        {
            final Map<Integer, AbstractDataPoller> values = entry.getValue();
            for (final AbstractDataPoller poller : values.values())
                poller.stopMe();
        }
        canContinue = false;
    }

    @Override
    public boolean canContinue()
    {
        return canContinue;
    }

    @Override
    public boolean processNow()
    {
        return false;
    }

}