package com.hazelcast.jet.examples.jobpriorities;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;

import java.util.Arrays;

import static java.util.concurrent.TimeUnit.SECONDS;

public class JobPriorities {

    private static final String RESULT_MAP_NAME = "results-by-job";
    private static final int DURATION_SECONDS = 60;

    public static void main(String[] args) throws Exception {
        JetInstance jet = Jet.bootstrappedInstance();
        new JobPrioritiesGui(jet.getMap(RESULT_MAP_NAME));

        try {
            Job[] jobs = Arrays.stream(args)
                    .mapToLong(Long::parseLong)
                    .mapToObj(priority -> priority + ":job")
                    .map(jobName -> jet.newJob(buildPipeline(), new JobConfig().setName(jobName)))
                    .toArray(Job[]::new);

            SECONDS.sleep(DURATION_SECONDS);

            Arrays.stream(jobs).forEach(job -> {
                job.cancel();
                job.join();
            });
        } finally {
            Jet.shutdownAll();
        }
    }

    private static Pipeline buildPipeline() {
        Pipeline p = Pipeline.create();
        //todo: define pipeline here
        return p;
    }

}
