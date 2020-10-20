package com.hazelcast.jet.examples.jobpriorities;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.datamodel.WindowResult;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.WindowDefinition;
import com.hazelcast.map.IMap;

import static java.util.concurrent.TimeUnit.SECONDS;

public class Main {

    public static void main(String[] args) throws InterruptedException {
        JetConfig jetConfig = new JetConfig();
        jetConfig.getInstanceConfig().setCooperativeThreadCount(6);
        JetInstance jet = Jet.newJetInstance(jetConfig);
        IMap<String, Long> map = jet.getMap("result-map");

        Job job = jet.newJob(buildPipeline("1:job1"));

        SECONDS.sleep(5);

        job.cancel();

        while (true) {
            System.out.println(map.get("job1"));
            SECONDS.sleep(1);
        }
    }

    private static Pipeline buildPipeline(String jobName) {
        String jobNameWithoutPriority = jobName.substring(jobName.indexOf(':') + 1);
        Pipeline p = Pipeline.create();
        p.readFrom(JobPriorities.modifiedLongStream())
                .withNativeTimestamps(0)
                .window(WindowDefinition.tumbling(2000))
                .aggregate(AggregateOperations.counting())
                .writeTo(Sinks.mapWithMerging("result-map", result -> jobNameWithoutPriority, WindowResult::result, Long::sum));
        return p;
    }
}
