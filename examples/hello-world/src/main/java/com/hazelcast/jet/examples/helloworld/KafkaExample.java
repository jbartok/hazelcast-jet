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

package com.hazelcast.jet.examples.helloworld;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.kafka.KafkaSources;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map.Entry;
import java.util.Properties;

public class KafkaExample {

    public static void main(String[] args) {
        Pipeline p = Pipeline.create();
        p.readFrom(KafkaSources.kafka(props(
                "bootstrap.servers", "localhost:9092",
                "key.deserializer", StringDeserializer.class.getCanonicalName(),
                "value.deserializer", StringDeserializer.class.getCanonicalName(),
                "auto.offset.reset", "earliest"),
                "my-topic"))
         .withoutTimestamps()
         .setLocalParallelism(1)
         .map(Entry::getValue)
         .writeTo(Sinks.logger());

        JetInstance jet = Jet.bootstrappedInstance();
        JobConfig config = new JobConfig();
        config.setName("kafka");
        config.setPublishLogs(true);
        jet.newJob(p, config);
    }

    private static Properties props(String... kvs) {
        final Properties props = new Properties();
        for (int i = 0; i < kvs.length; ) {
            props.setProperty(kvs[i++], kvs[i++]);
        }
        return props;
    }
}
