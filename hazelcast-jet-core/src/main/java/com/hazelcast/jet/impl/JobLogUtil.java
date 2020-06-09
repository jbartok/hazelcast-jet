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
import com.hazelcast.jet.Util;
import com.hazelcast.jet.impl.observer.ObservableImpl;
import com.hazelcast.logging.LogEvent;
import com.hazelcast.ringbuffer.OverflowPolicy;
import com.hazelcast.ringbuffer.Ringbuffer;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.logging.Level;

import static com.hazelcast.jet.impl.util.Util.toLocalTime;

public final class JobLogUtil {

    private JobLogUtil() {
    }

    public static String ringbufferName(long jobId) {
        return ringbufferName(Util.idToString(jobId));
    }

    public static String ringbufferName(String jobId) {
        return ObservableImpl.JET_OBSERVABLE_NAME_PREFIX + "logs." + jobId;
    }

    public static String format(LogEvent event) {
        Address member = event.getMember().getAddress();
        long timestamp = event.getLogRecord().getMillis();
        String message = event.getLogRecord().getMessage();
        Level level = event.getLogRecord().getLevel();
        return format(member, timestamp, message, level, null);
    }

    public static String format(Address member, String message, Level level) {
        return format(member, message, level, null);
    }

    public static String format(Address member, String message, Level level, Throwable t) {
        return format(member, System.currentTimeMillis(), message, level, t);
    }

    public static String format(Address member, long timestamp, String message, Level level, Throwable t) {
        StringBuilder sb = new StringBuilder()
                .append(member)
                .append("@")
                .append(toLocalTime(timestamp))
                .append(" [")
                .append(level)
                .append("] - ")
                .append(message);
        if (t != null) {
            StringWriter stringWriter = new StringWriter();
            t.printStackTrace(new PrintWriter(stringWriter));

            sb.append("\n").append(t.toString()).append(stringWriter.toString());
        }
        return sb.toString();
    }

    public static void addToRingbuffer(Ringbuffer<String> ringbuffer, String message) {
        ringbuffer.addAsync(message, OverflowPolicy.OVERWRITE);
    }
}
