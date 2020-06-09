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

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.logging.AbstractLogger;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LogEvent;
import com.hazelcast.ringbuffer.Ringbuffer;

import java.util.logging.Level;

public class JobLogger extends AbstractLogger {

    private final Address member;
    private final ILogger delegate;
    private final Ringbuffer<String> ringbuffer;

    public JobLogger(Member member, ILogger delegate, long jobId, JetInstance instance) {
        this.member = member.getAddress();
        this.delegate = delegate;
        this.ringbuffer = instance.getHazelcastInstance().getRingbuffer(JobLogUtil.ringbufferName(jobId));
    }

    @Override
    public void log(Level level, String message) {
        if (isLoggable(level)) {
            delegate.log(level, message);
            JobLogUtil.addToRingbuffer(ringbuffer, JobLogUtil.format(member, message, level));
        }
    }

    @Override
    public void log(Level level, String message, Throwable thrown) {
        if (isLoggable(level)) {
            delegate.log(level, message, thrown);
            JobLogUtil.addToRingbuffer(ringbuffer, JobLogUtil.format(member, message, level, thrown));
        }
    }

    @Override
    public void log(LogEvent event) {
        if (isLoggable(event.getLogRecord().getLevel())) {
            delegate.log(event);
            JobLogUtil.addToRingbuffer(ringbuffer, JobLogUtil.format(event));
        }
    }

    @Override
    public Level getLevel() {
        return delegate.getLevel();
    }

    @Override
    public boolean isLoggable(Level level) {
        return delegate.isLoggable(level);
    }
}
