/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.jet.kinesis.impl;

import com.hazelcast.jet.retry.RetryStrategy;
import com.hazelcast.logging.ILogger;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryResponse;
import software.amazon.awssdk.services.kinesis.model.StreamDescriptionSummary;

import javax.annotation.Nonnull;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ShardCountMonitor extends AbstractShardWorker {

    /**
     * DescribeStreamSummary operations are limited to 20 per second, per account.
     */
    private static final int DESCRIBE_STREAM_OPERATIONS_ALLOWED_PER_SECOND = 20;

    /**
     * We don't want to issue describe stream operations at the peak allowed rate.
     */
    private static final double RATIO_OF_DESCRIBE_STREAM_RATE_UTILIZED = 0.1;

    private final AtomicInteger shardCount;
    private final RandomizedRateTracker describeStreamRateTracker;
    private final RetryTracker describeStreamRetryTracker;

    private Future<DescribeStreamSummaryResponse> describeStreamResponse;
    private long nextDescribeStreamTime;

    public ShardCountMonitor(
            int totalInstances,
            KinesisAsyncClient client,
            String stream,
            RetryStrategy retryStrategy,
            ILogger logger
    ) {
        super(client, stream, logger);
        this.shardCount = new AtomicInteger();
        this.describeStreamRetryTracker = new RetryTracker(retryStrategy);
        this.describeStreamRateTracker = initRandomizedTracker(totalInstances);
        this.nextDescribeStreamTime = System.nanoTime();
    }

    private ShardCountMonitor(AtomicInteger shardCount) {
        super(null, null, null);
        this.shardCount = shardCount;
        describeStreamRateTracker = null;
        describeStreamRetryTracker = null;
    }


    public void run() {
        if (describeStreamResponse == null) {
            initDescribeStream();
        } else {
            checkForStreamDescription();
        }
    }

    public ShardCountMonitor noop() {
        return new NoopShardCountMonitor(shardCount);
    }

    public int shardCount() {
        return shardCount.get();
    }

    private void initDescribeStream() {
        long currentTime = System.nanoTime();
        if (currentTime < nextDescribeStreamTime) {
            return;
        }
        describeStreamResponse = helper.describeStreamSummaryAsync();
        nextDescribeStreamTime = currentTime + describeStreamRateTracker.next();
    }

    private void checkForStreamDescription() {
        if (describeStreamResponse.isDone()) {
            DescribeStreamSummaryResponse response;
            try {
                response = helper.readResult(describeStreamResponse);
            } catch (SdkClientException e) {
                dealWithDescribeStreamFailure(e);
                return;
            } catch (Throwable t) {
                throw rethrow(t);
            } finally {
                describeStreamResponse = null;
            }

            describeStreamRetryTracker.reset();

            StreamDescriptionSummary streamDescription = response.streamDescriptionSummary();
            if (streamDescription == null) {
                return;
            }

            Integer newShardCount = streamDescription.openShardCount();
            if (newShardCount == null) {
                return;
            }

            int oldShardCount = shardCount.getAndSet(newShardCount);
            if (oldShardCount != newShardCount) {
                logger.info(String.format("Updated shard count for stream %s: %d", stream, newShardCount));
            }
        }
    }

    private void dealWithDescribeStreamFailure(@Nonnull Exception failure) {
        describeStreamRetryTracker.attemptFailed();
        if (describeStreamRetryTracker.shouldTryAgain()) {
            long timeoutMillis = describeStreamRetryTracker.getNextWaitTimeMillis();
            logger.warning(String.format("Failed obtaining stream description, retrying in %d ms. Cause: %s",
                    timeoutMillis, failure.getMessage()));
            nextDescribeStreamTime = System.nanoTime() + MILLISECONDS.toNanos(timeoutMillis);
        } else {
            throw rethrow(failure);
        }

    }

    @Nonnull
    private static RandomizedRateTracker initRandomizedTracker(int totalInstances) {
        // The maximum rate at which DescribeStreamSummary operations can be
        // performed on a data stream is 20/second and we need to enforce this,
        // even while we are issuing them from multiple processors in parallel
        return new RandomizedRateTracker(SECONDS.toNanos(1) * totalInstances,
                (int) (DESCRIBE_STREAM_OPERATIONS_ALLOWED_PER_SECOND * RATIO_OF_DESCRIBE_STREAM_RATE_UTILIZED));
    }

    private static final class NoopShardCountMonitor extends ShardCountMonitor {

        private NoopShardCountMonitor(AtomicInteger shardCount) {
            super(shardCount);
        }

        @Override
        public void run() {
        }
    }

}
