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

package com.hazelcast.jet.slf4j;

import com.hazelcast.cluster.Member;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.impl.JobLogUtil;
import com.hazelcast.jet.impl.deployment.JetClassLoader;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.ringbuffer.Ringbuffer;
import org.slf4j.helpers.MarkerIgnoringBase;

import java.util.Map;
import java.util.logging.Level;

/**
 *
 */
public class JetJobLogger extends MarkerIgnoringBase {

    private final ILogger logger = Logger.getLogger(JetJobLogger.class);

    private final Map<String, Ringbuffer<String>> ringbuffers;
    private final ILogger delegate;

    /**
     *
     * @param s
     * @param ringbuffers
     */
    public JetJobLogger(String s, Map<String, Ringbuffer<String>> ringbuffers) {
        delegate = Logger.getLogger(s);
        this.ringbuffers = ringbuffers;
    }

    @Override
    public boolean isInfoEnabled() {
        return delegate.isInfoEnabled();
    }

    @Override
    public void info(String msg) {
        if (delegate.isInfoEnabled()) {
            addToRingbuffer(msg, Level.INFO, null);
            delegate.info(msg);
        }
    }

    @Override
    public void info(String format, Object arg) {
        if (delegate.isInfoEnabled()) {
            String message = String.format(format.replaceAll("\\{}", "%s"), arg);

            addToRingbuffer(message, Level.INFO, null);
            delegate.info(message);
        }
    }

    @Override
    public void info(String format, Object arg1, Object arg2) {
        if (delegate.isInfoEnabled()) {
            String message = String.format(format.replaceAll("\\{}", "%s"), arg1, arg2);

            addToRingbuffer(message, Level.INFO, null);
            delegate.info(message);
        }
    }

    @Override
    public void info(String format, Object... arguments) {
        if (delegate.isInfoEnabled()) {
            String message = String.format(format.replaceAll("\\{}", "%s"), arguments);

            addToRingbuffer(message, Level.INFO, null);
            delegate.info(message);
        }
    }

    @Override
    public void info(String message, Throwable t) {
        if (delegate.isInfoEnabled()) {
            addToRingbuffer(message, Level.INFO, t);
            delegate.info(message);
        }
    }

    @Override
    public boolean isDebugEnabled() {
        return delegate.isFineEnabled();
    }

    @Override
    public void debug(String msg) {
        if (delegate.isFineEnabled()) {
            addToRingbuffer(msg, Level.FINE, null);
            delegate.fine(msg);
        }
    }

    @Override
    public void debug(String format, Object arg) {
        if (delegate.isFineEnabled()) {
            String message = String.format(format.replaceAll("\\{}", "%s"), arg);

            addToRingbuffer(message, Level.FINE, null);
            delegate.fine(message);
        }
    }

    @Override
    public void debug(String format, Object arg1, Object arg2) {
        if (delegate.isFineEnabled()) {
            String message = String.format(format.replaceAll("\\{}", "%s"), arg1, arg2);

            addToRingbuffer(message, Level.FINE, null);
            delegate.fine(message);
        }
    }

    @Override
    public void debug(String format, Object... arguments) {
        if (delegate.isFineEnabled()) {
            String message = String.format(format.replaceAll("\\{}", "%s"), arguments);

            addToRingbuffer(message, Level.FINE, null);
            delegate.fine(message);
        }
    }

    @Override
    public void debug(String message, Throwable t) {
        if (delegate.isFineEnabled()) {
            addToRingbuffer(message, Level.FINE, t);
            delegate.fine(message);
        }
    }

    private void addToRingbuffer(String message, Level level, Throwable throwable) {
        Thread thread = Thread.currentThread();
        ClassLoader contextClassLoader = thread.getContextClassLoader();
        if (contextClassLoader instanceof JetClassLoader) {
            JetClassLoader jetClassLoader = (JetClassLoader) contextClassLoader;
            String jobId = Util.idToString(jetClassLoader.getJobId());
            Member member = jetClassLoader.getMember();

            Ringbuffer<String> ringbuffer = ringbuffers.get(jobId);
            if (ringbuffer != null) {
                JobLogUtil.addToRingbuffer(ringbuffer, JobLogUtil.format(member.getAddress(), message, level, throwable));
            } else {
                logger.warning("Logging ringbuffer for " + jobId + " does not exist.");
            }
        } else {
            logger.warning("Classloader " + contextClassLoader + " is not an instance of JetClassLoader");
        }
    }

    /////////////// TODO
    @Override
    public boolean isTraceEnabled() {
        return false;
    }

    @Override
    public void trace(String msg) {

    }

    @Override
    public void trace(String format, Object arg) {

    }

    @Override
    public void trace(String format, Object arg1, Object arg2) {

    }

    @Override
    public void trace(String format, Object... arguments) {

    }

    @Override
    public void trace(String msg, Throwable t) {

    }

    @Override
    public boolean isWarnEnabled() {
        return false;
    }

    @Override
    public void warn(String msg) {

    }

    @Override
    public void warn(String format, Object arg) {

    }

    @Override
    public void warn(String format, Object... arguments) {

    }

    @Override
    public void warn(String format, Object arg1, Object arg2) {

    }

    @Override
    public void warn(String msg, Throwable t) {

    }

    @Override
    public boolean isErrorEnabled() {
        return false;
    }

    @Override
    public void error(String msg) {

    }

    @Override
    public void error(String format, Object arg) {

    }

    @Override
    public void error(String format, Object arg1, Object arg2) {

    }

    @Override
    public void error(String format, Object... arguments) {

    }

    @Override
    public void error(String msg, Throwable t) {

    }
}
