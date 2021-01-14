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
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.Shard;

import javax.annotation.Nonnull;
import java.math.BigInteger;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static com.hazelcast.jet.kinesis.impl.KinesisHelper.shardBelongsToRange;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;

public class RangeMonitor extends AbstractShardWorker {

    /**
     * ListStreams operations are limited to 100 per second, per data stream
     */
    private static final int SHARD_LISTINGS_ALLOWED_PER_SECOND = 100;

    /**
     * We don't want to issue shard listing requests at the peak allowed rate.
     */
    private static final double RATIO_OF_SHARD_LISTING_RATE_UTILIZED = 0.1;

    private final ShardTracker shardTracker;
    private final HashRange memberHashRange;
    private final ShardQueue[] shardQueues;
    private final RandomizedRateTracker listShardsRateTracker;
    private final RetryTracker listShardRetryTracker;

    private String nextToken;
    private Future<ListShardsResponse> listShardsResponse;
    private long nextListShardsTimeMs;

    public RangeMonitor(
            int totalInstances,
            KinesisAsyncClient client,
            String stream,
            HashRange memberHashRange,
            HashRange[] rangePartitions,
            ShardQueue[] shardQueues,
            RetryStrategy retryStrategy,
            ILogger logger
    ) {
        super(client, stream, logger);
        this.memberHashRange = memberHashRange;
        this.shardTracker = new ShardTracker(rangePartitions);
        this.shardQueues = shardQueues;
        this.listShardRetryTracker = new RetryTracker(retryStrategy);
        this.listShardsRateTracker = initRandomizedTracker(totalInstances);
        this.nextListShardsTimeMs = System.currentTimeMillis();
    }

    public void run() {
        long currentTimeMs = System.currentTimeMillis();
        if (listShardsResponse == null) {
            initShardListing(currentTimeMs);
        } else {
            if (listShardsResponse.isDone()) {
                ListShardsResponse response;
                try {
                    response = helper.readResult(listShardsResponse);
                } catch (SdkClientException e) {
                    dealWithListShardsFailure(e);
                    return;
                } catch (Throwable t) {
                    throw rethrow(t);
                } finally {
                    listShardsResponse = null;
                }

                listShardRetryTracker.reset();

                checkForNewShards(currentTimeMs, response);

                nextToken = response.nextToken();
                if (nextToken == null) {
                    checkForExpiredShards(currentTimeMs);
                }
            }
        }
    }

    private void initShardListing(long currentTimeMs) {
        if (currentTimeMs < nextListShardsTimeMs) {
            return;
        }
        listShardsResponse = helper.listAllShardsAsync(nextToken);
        nextListShardsTimeMs = currentTimeMs + listShardsRateTracker.next();
    }

    private void checkForNewShards(long currentTimeMs, ListShardsResponse response) {
        Set<Shard> shards = response.shards().stream()
                .filter(shard -> shardBelongsToRange(shard, memberHashRange))
                .collect(Collectors.toSet());
        Map<Shard, Integer> newShards = shardTracker.markDetections(shards, currentTimeMs);

        if (!newShards.isEmpty()) {
            logger.info("New shards detected: " +
                    newShards.keySet().stream().map(Shard::shardId).collect(joining(", ")));

            for (Map.Entry<Shard, Integer> e : newShards.entrySet()) {
                Shard shard = e.getKey();
                int owner = e.getValue();
                shardQueues[owner].addAdded(shard);
            }
        }
    }

    private void checkForExpiredShards(long currentTimeMs) {
        Map<String, Integer> expiredShards = shardTracker.removeExpiredShards(currentTimeMs);
        for (Map.Entry<String, Integer> e : expiredShards.entrySet()) {
            String shardId = e.getKey();
            int owner = e.getValue();
            logger.info("Expired shard detected: " + shardId);
            shardQueues[owner].addExpired(shardId);
        }
    }

    public void addKnownShard(String shardId, BigInteger startingHashKey) {
        shardTracker.addUndetected(shardId, startingHashKey, System.currentTimeMillis());
    }

    private void dealWithListShardsFailure(@Nonnull Exception failure) {
        nextToken = null;

        listShardRetryTracker.attemptFailed();
        if (listShardRetryTracker.shouldTryAgain()) {
            long timeoutMillis = listShardRetryTracker.getNextWaitTimeMillis();
            logger.warning(String.format("Failed listing shards, retrying in %d ms. Cause: %s",
                    timeoutMillis, failure.getMessage()));
            nextListShardsTimeMs = System.currentTimeMillis() + timeoutMillis;
        } else {
            throw rethrow(failure);
        }

    }

    @Nonnull
    private static RandomizedRateTracker initRandomizedTracker(int totalInstances) {
        // The maximum rate at which ListStreams operations can be performed on
        // a data stream is 100/second and we need to enforce this, even while
        // we are issuing them from multiple processors in parallel
        return new RandomizedRateTracker(SECONDS.toMillis(1) * totalInstances,
                (int) (SHARD_LISTINGS_ALLOWED_PER_SECOND * RATIO_OF_SHARD_LISTING_RATE_UTILIZED));
    }

}
