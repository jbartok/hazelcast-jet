package com.hazelcast.jet.examples.jobpriorities;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.datamodel.WindowResult;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.WindowDefinition;
import com.hazelcast.jet.pipeline.test.ModifiedLongStreamSourceP;
import com.hazelcast.map.IMap;

import java.util.Arrays;
import java.util.concurrent.CancellationException;

import static java.util.concurrent.TimeUnit.SECONDS;

public class JobPriorities {

    private static final String RESULT_MAP_NAME = "results-by-job";

    public static void main(String[] args) throws Exception {
        try {
            JetConfig jetConfig = new JetConfig();
            jetConfig.getInstanceConfig().setCooperativeThreadCount(6);
            JetInstance jet = Jet.newJetInstance(jetConfig);

            while (true) {
                run(jet, 10, 1, 1, 1, 1, 1, 1);
                run(jet, 10, 1, 1, 1, 1, 1, 2);
                run(jet, 10, 1, 1, 1, 1, 1, 3);
                run(jet, 15, 1, 2, 2, 3, 3, 4);
            }
        } finally {
            Jet.shutdownAll();
        }
    }

    private static void run(JetInstance jet, int durationSeconds, long... priorities) throws InterruptedException {
        IMap<String, Long> map = jet.getMap(RESULT_MAP_NAME);

        JobPrioritiesGui gui = new JobPrioritiesGui(map, priorities.length);

        Job[] jobs = new Job[priorities.length];

        for (int i = 0; i < priorities.length; i++) {
            String jobName = priorities[i] + ":job" + i;
            jobs[i] = jet.newJob(buildPipeline(jobName), new JobConfig().setName(jobName));
        }

        SECONDS.sleep(durationSeconds);

        gui.stop();

        Arrays.stream(jobs).forEach(job -> {
            try {
                job.cancel();
                job.join();
            } catch (CancellationException ce) {
                //ignore
            }
        });

        map.clear();
    }

    private static Pipeline buildPipeline(String jobName) {
        Pipeline p = Pipeline.create();
        p.readFrom(modifiedLongStream())
                .withNativeTimestamps(0)
                .window(WindowDefinition.tumbling(50))
                .aggregate(AggregateOperations.counting())
                .writeTo(Sinks.mapWithMerging(RESULT_MAP_NAME, result -> jobName, WindowResult::result, Long::sum));
        return p;
    }

    public static StreamSource<Long> modifiedLongStream() {
        return Sources.streamFromProcessorWithWatermarks("longStream",
                true,
                eventTimePolicy -> ProcessorMetaSupplier.of(() -> new ModifiedLongStreamSourceP(eventTimePolicy))
        );
    }

}
