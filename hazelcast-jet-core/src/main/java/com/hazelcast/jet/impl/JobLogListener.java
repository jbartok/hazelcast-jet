package com.hazelcast.jet.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.impl.observer.ObservableImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LogEvent;
import com.hazelcast.logging.LogListener;
import com.hazelcast.logging.Logger;
import com.hazelcast.ringbuffer.OverflowPolicy;
import com.hazelcast.ringbuffer.Ringbuffer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.jet.impl.util.Util.toLocalTime;

public class JobLogListener implements LogListener {

    private static final ILogger logger = Logger.getLogger(JobLogListener.class);

    private final HazelcastInstance instance;
    private final Map<String, Ringbuffer<String>> jobIds = new ConcurrentHashMap<>();

    public JobLogListener(HazelcastInstance instance) {
        this.instance = instance;
    }

    public void addJobId(long jobId) {
        String stringJobId = Util.idToString(jobId);
        logger.info("Register logger for jobId=" + stringJobId);
        jobIds.computeIfAbsent(stringJobId, missingJobId ->
                instance.getRingbuffer(ObservableImpl.JET_OBSERVABLE_NAME_PREFIX + "logs." + stringJobId));
    }

    public void removeJobId(long jobId) {
        String stringJobId = Util.idToString(jobId);
        jobIds.remove(stringJobId);
        logger.info("Deregister logger for jobId=" + stringJobId);
    }

    @Override
    public void log(LogEvent logEvent) {
        for (String jobId : jobIds.keySet()) {
            if (logEvent.getLogRecord().getMessage().contains(jobId)) {
                Ringbuffer<String> ringbuffer = jobIds.get(jobId);
                if (ringbuffer != null) {
                    ringbuffer.addAsync(format(logEvent), OverflowPolicy.OVERWRITE);
                } else {
                    logger.warning("Null logging ringbuffer for jobId=" + jobId);
                    new Throwable().printStackTrace(); // just for discovering stuff quickly :-)
                }
            }
        }
    }

    private String format(LogEvent event) {
        Address member = event.getMember().getAddress();
        String timestamp = toLocalTime(event.getLogRecord().getMillis());
        String message = event.getLogRecord().getMessage();
        return member + "@" + timestamp + " - " + message;
    }
}
