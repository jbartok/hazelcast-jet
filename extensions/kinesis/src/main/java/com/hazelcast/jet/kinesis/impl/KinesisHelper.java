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

import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryResponse;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardFilter;
import software.amazon.awssdk.services.kinesis.model.ShardFilterType;
import com.hazelcast.jet.JetException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigInteger;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KinesisHelper {

    private final KinesisAsyncClient client;
    private final String stream;

    public KinesisHelper(KinesisAsyncClient client, String stream) {
        this.client = client;
        this.stream = stream;
    }

    public static boolean shardActive(@Nonnull Shard shard) {
        String endingSequenceNumber = shard.sequenceNumberRange().endingSequenceNumber();
        return endingSequenceNumber == null;
        //need to rely on this hack, because shard filters don't seem to work, on the mock at least ...
    }

    public static boolean shardBelongsToRange(@Nonnull Shard shard, @Nonnull HashRange range) {
        BigInteger startingHashKey = new BigInteger(shard.hashKeyRange().startingHashKey());
        return shardBelongsToRange(startingHashKey, range);
    }

    public static boolean shardBelongsToRange(@Nonnull BigInteger startingHashKey, @Nonnull HashRange range) {
        return range.contains(startingHashKey);
    }

    public static ListShardsRequest listAllShardsRequest(
            String stream,
            @Nullable String nextToken,
            ShardFilterType filterType
    ) {
        ListShardsRequest.Builder request = ListShardsRequest.builder();
        if (nextToken == null) {
            request.streamName(stream);
        } else {
            request.nextToken(nextToken);
        }

        //include all the shards within the retention period of the data stream
        request.shardFilter(ShardFilter.builder().type(filterType).build());

        return request.build();
    }

    public Future<DescribeStreamSummaryResponse> describeStreamSummaryAsync() {
        DescribeStreamSummaryRequest request = DescribeStreamSummaryRequest.builder().streamName(stream).build();
        return client.describeStreamSummary(request);
    }

    public Future<ListShardsResponse> listAllShardsAsync(String nextToken) {
        ShardFilterType filterType = ShardFilterType.FROM_TRIM_HORIZON;
        //all shards within the retention period (including closed, excluding expired)

        ListShardsRequest request = listAllShardsRequest(stream, nextToken, filterType);
        return client.listShards(request);
    }

    public Future<GetShardIteratorResponse> getShardIteratorAsync(GetShardIteratorRequest request) {
        return client.getShardIterator(request);
    }

    public Future<GetRecordsResponse> getRecordsAsync(String shardIterator) {
        GetRecordsRequest request = GetRecordsRequest.builder().shardIterator(shardIterator).build();
        return client.getRecords(request);
    }

    public Future<PutRecordsResponse> putRecordsAsync(Collection<PutRecordsRequestEntry> entries) {
        PutRecordsRequest request = PutRecordsRequest.builder().streamName(stream).records(entries).build();
        return client.putRecords(request);
    }

    public <T> T readResult(Future<T> future) throws Throwable {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new JetException("Interrupted while waiting for results");
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }
}
