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
import com.hazelcast.instance.SimpleMemberImpl;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.jet.Util;
import com.hazelcast.logging.LogEvent;
import com.hazelcast.ringbuffer.OverflowPolicy;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.version.MemberVersion;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.net.InetSocketAddress;
import java.util.logging.Level;
import java.util.logging.LogRecord;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class JobLogListenerTest {

    private JobLogListener jobLogListener;

    private Ringbuffer ringbuffer;

    private ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);

    @Before
    public void setUp() throws Exception {
        ringbuffer = Mockito.mock(Ringbuffer.class);
        HazelcastInstance instance = mock(HazelcastInstance.class);
        when(instance.getRingbuffer(any()))
                .thenReturn(ringbuffer);

        jobLogListener = new JobLogListener(instance);
    }

    @Test
    public void log() {
        jobLogListener.addJobId(42);

        String message = "Hello " + Util.idToString(42);
        LogEvent logEvent = logEvent(message);
        jobLogListener.log(logEvent);

        verify(ringbuffer).addAsync(captor.capture(), eq(OverflowPolicy.OVERWRITE));
        String capturedMessage = captor.getValue();
        assertThat(capturedMessage).contains(message);
    }

    @Test
    public void shouldMatchMultipleIds() {
        jobLogListener.addJobId(42);
        jobLogListener.addJobId(43);

        String message = "Hello " + Util.idToString(42);
        LogEvent logEvent = logEvent(message);
        jobLogListener.log(logEvent);

        verify(ringbuffer).addAsync(captor.capture(), eq(OverflowPolicy.OVERWRITE));
        String capturedMessage = captor.getValue();
        assertThat(capturedMessage).contains(message);
    }

    @NotNull
    private LogEvent logEvent(String msg) {
        LogRecord record = new LogRecord(Level.INFO, msg);
        return new LogEvent(record,
                new SimpleMemberImpl(new MemberVersion(4, 1, 1),
                        UuidUtil.newUnsecureUUID(),
                        new InetSocketAddress("127.0.0.1", 5701)));
    }
}
