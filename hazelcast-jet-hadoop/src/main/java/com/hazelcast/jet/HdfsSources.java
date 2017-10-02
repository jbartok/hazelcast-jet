/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet;

import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.impl.connector.hadoop.ReadHdfsP.MetaSupplier;
import org.apache.hadoop.mapred.JobConf;

import javax.annotation.Nonnull;
import java.util.Map.Entry;

import static com.hazelcast.jet.impl.connector.hadoop.SerializableJobConf.asSerializable;

/**
 * Contains factory methods for Apache Hadoop HDFS sources.
 */
public final class HdfsSources {

    private HdfsSources() {
    }

    /**
     * Returns a source that reads records from Apache Hadoop HDFS and emits
     * the results of transforming each record (a key-value pair) with the
     * supplied mapping function.
     * <p>
     * This source splits and balances the input data among Jet {@link
     * com.hazelcast.jet.core.Processor processors}, doing its best to achieve
     * data locality. To this end the Jet cluster topology should be aligned
     * with Hadoop's &mdash; on each Hadoop member there should be a Jet
     * member.
     *
     * @param <K> key type of the records
     * @param <V> value type of the records
     * @param <E> the type of the emitted value

     * @param jobConf JobConf for reading files with the appropriate input format and path
     * @param mapper  mapper which can be used to map the key and value to another value
     */
    @Nonnull
    public static <K, V, E> Source<E> readHdfs(
            @Nonnull JobConf jobConf, @Nonnull DistributedBiFunction<K, V, E> mapper
    ) {
        return Sources.fromProcessor("readHdfs", new MetaSupplier<>(asSerializable(jobConf), mapper));
    }

    /**
     * Convenience for {@link #readHdfs(JobConf, DistributedBiFunction)}
     * with {@link java.util.Map.Entry} as its output type.
     */
    @Nonnull
    public static <K, V> Source<Entry<K, V>> readHdfs(@Nonnull JobConf jobConf) {
        return readHdfs(jobConf, (DistributedBiFunction<K, V, Entry<K, V>>) Util::entry);
    }
}