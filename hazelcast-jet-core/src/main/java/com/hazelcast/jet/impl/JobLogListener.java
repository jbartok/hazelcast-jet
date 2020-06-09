/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Util;
import com.hazelcast.logging.LogEvent;
import com.hazelcast.logging.LogListener;
import com.hazelcast.ringbuffer.Ringbuffer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class JobLogListener implements LogListener {

    private final HazelcastInstance instance;
    private final Map<String, Ringbuffer<String>> buffers = new ConcurrentHashMap<>();

    public JobLogListener(HazelcastInstance instance) {
        this.instance = instance;
    }

    public void addJobId(long jobId) {
        buffers.computeIfAbsent(Util.idToString(jobId),
                id -> instance.getRingbuffer(JobLogUtil.ringbufferName(id)));
    }

    public void removeJobId(long jobId) {
        buffers.remove(Util.idToString(jobId));
    }

    @Override
    public void log(LogEvent logEvent) {
        for (Map.Entry<String, Ringbuffer<String>> entry : buffers.entrySet()) {
            String jobId = entry.getKey();
            boolean needsToBeLogged = logEvent.getLogRecord().getMessage().contains(jobId);
            if (needsToBeLogged) {
                JobLogUtil.addToRingbuffer(entry.getValue(), JobLogUtil.format(logEvent));
            }
        }
    }

}
