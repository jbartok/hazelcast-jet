package com.hazelcast.jet.examples.jobpriorities;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;

import java.util.Arrays;

import static com.hazelcast.jet.Util.entry;
import static java.util.concurrent.TimeUnit.SECONDS;

public class JobPriorities {

    private static final String RESULT_MAP_NAME = "results-by-job";
    private static final int DURATION_SECONDS = 60;

    public static void main(String[] args) throws Exception {
        JetInstance jet = Jet.bootstrappedInstance();
        JobPrioritiesGui gui = new JobPrioritiesGui(jet.getMap(RESULT_MAP_NAME));

        try {
            Job[] jobs = new Job[args.length];

            for (int i = 0; i < args.length; i++) {
                long priority = Long.parseLong(args[i]);
                String jobName = priority + ":job" + i;
                jobs[i] = jet.newJob(buildPipeline(jobName), new JobConfig().setName(jobName));
            }

            SECONDS.sleep(DURATION_SECONDS);

            Arrays.stream(jobs).forEach(job -> {
                job.cancel();
                job.join();
            });

            gui.stop();
        } finally {
            Jet.shutdownAll();
        }
    }

    private static Pipeline buildPipeline(String jobName) {
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.itemStream(2_000))
                .withIngestionTimestamps()
                .rollingAggregate(AggregateOperations.counting())
                .setLocalParallelism(2)
                .map(count -> entry(jobName, count))
                .setLocalParallelism(2)
                .writeTo(Sinks.map(RESULT_MAP_NAME));
        return p;
    }

}
