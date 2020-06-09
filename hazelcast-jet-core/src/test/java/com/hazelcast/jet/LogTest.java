package com.hazelcast.jet;

import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.impl.observer.ObservableImpl;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.logging.LogEvent;
import org.junit.Test;

import java.util.UUID;

public class LogTest extends JetTestSupport {

    @Test
    public void name() {
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(1, 2, 3, 4))
         .writeTo(Sinks.logger());

        JetInstance member = createJetMember();
        Job job = member.newJob(p, new JobConfig().setPublishLogs(true));


        Observable<String> observable = member.getObservable("logs." + job.getIdString());
        UUID uuid = observable.addObserver(x -> {
            System.out.println(x);
        });


        job.join();

        observable.removeObserver(uuid);


    }
}
