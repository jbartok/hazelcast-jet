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

package com.hazelcast.jet.pipeline.test;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.AppendableTraverser;
import com.hazelcast.jet.core.EventTimeMapper;
import com.hazelcast.jet.core.EventTimePolicy;

import javax.annotation.Nonnull;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Implements the {@link TestSources#longStream} source.
 *
 * @since 4.3
 */
public class ModifiedLongStreamSourceP extends AbstractProcessor {

    private final EventTimeMapper<? super Long> eventTimeMapper;
    private long totalParallelism;

    private long valueToEmit;
    private Traverser<Object> traverser = new AppendableTraverser<>(2);

    public ModifiedLongStreamSourceP(EventTimePolicy<? super Long> eventTimePolicy) {
        this.eventTimeMapper = new EventTimeMapper<>(eventTimePolicy);
        eventTimeMapper.addPartitions(1);
    }

    @Override
    protected void init(@Nonnull Context context) {
        totalParallelism = context.totalParallelism();
        valueToEmit = context.globalProcessorIndex();
    }

    @Override
    public boolean complete() {
        long nowNanoTime = NANOSECONDS.toMillis(System.nanoTime());
        if (traverser == null) {
            traverser = eventTimeMapper.flatMapEvent(nowNanoTime, valueToEmit, 0, nowNanoTime);
            valueToEmit += totalParallelism;
        }
        if (emitFromTraverser(traverser)) {
            traverser = null;
        }
        return false;
    }

}
