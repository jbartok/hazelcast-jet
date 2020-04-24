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

package com.hazelcast.jet.cdc.impl;

import com.hazelcast.jet.cdc.ChangeEvent;
import com.hazelcast.jet.cdc.ChangeEventElement;
import com.hazelcast.jet.cdc.Operation;
import com.hazelcast.jet.cdc.ParsingException;

import javax.annotation.Nonnull;
import java.util.Objects;

public class ChangeEventMongoImpl implements ChangeEvent { //todo: preper serialization

    private final String keyJson;
    private final String valueJson;

    private String json;
    private Long timestamp;
    private Operation operation;
    private ChangeEventElement key;
    private ChangeEventElement value;

    public ChangeEventMongoImpl(@Nonnull String keyJson, @Nonnull String valueJson) {
        this.keyJson = Objects.requireNonNull(keyJson, "keyJson");
        this.valueJson = Objects.requireNonNull(valueJson, "valueJson");
    }

    @Override
    public long timestamp() throws ParsingException {
        if (timestamp == null) {
            timestamp = value().getLong("__ts_ms")
                    .orElseThrow(() -> new ParsingException("No parsable timestamp field found"));
        }
        return timestamp;
    }

    @Override
    @Nonnull
    public Operation operation() throws ParsingException {
        if (operation == null) {
            operation = Operation.get(value().getString("__op").orElse(null));
        }
        return operation;
    }

    @Override
    @Nonnull
    public ChangeEventElement key() {
        if (key == null) {
            key = new ChangeEventElementMongoImpl(keyJson);
        }
        return key;
    }

    @Override
    @Nonnull
    public ChangeEventElement value() {
        if (value == null) {
            value = new ChangeEventElementMongoImpl(valueJson);
        }
        return value;
    }

    @Override
    @Nonnull
    public String asJson() {
        if (json == null) {
            json = String.format("key:{%s}, value:{%s}", keyJson, valueJson);
        }
        return json;
    }

    @Override
    public String toString() {
        return asJson();
    }

}
