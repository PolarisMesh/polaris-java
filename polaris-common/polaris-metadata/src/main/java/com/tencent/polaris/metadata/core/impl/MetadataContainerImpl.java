/*
 * Tencent is pleased to support the open source community by making Polaris available.
 *
 * Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.tencent.polaris.metadata.core.impl;

import com.tencent.polaris.metadata.core.*;
import com.tencent.polaris.metadata.core.manager.Utils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class MetadataContainerImpl implements MetadataContainer {

    private final Map<String, MetadataValue> values = new ConcurrentHashMap<>();

    private final String transitivePrefix;

    public MetadataContainerImpl(String transitivePrefix) {
        this.transitivePrefix = transitivePrefix;
    }

    @Override
    public void putMetadataStringValue(String key, String value, TransitiveType transitiveType) {
        values.put(key, new MetadataStringValueImpl(transitiveType, value));
    }

    @Override
    public void putMetadataMapValue(String key, String mapKey, String value, TransitiveType transitiveType) {
        MetadataValue metadataValue = values.computeIfAbsent(key, new Function<String, MetadataValue>() {
            @Override
            public MetadataValue apply(String s) {
                return new MetadataMapValueImpl(transitivePrefix);
            }
        });
        MetadataMapValue metadataMapValue = (MetadataMapValue) metadataValue;
        metadataMapValue.putMapValue(mapKey, new MetadataStringValueImpl(transitiveType, value));
    }

    @Override
    public MetadataValue getMetadataValue(String key) {
        return values.get(key);
    }

    public void iterateMetadataValues(BiConsumer<String, MetadataValue> iterator) {
        values.forEach(iterator);
    }

    @Override
    public Map<String, String> getAllTransitiveKeyValues() {
        Map<String, String> values = new HashMap<>();
        iterateMetadataValues(new BiConsumer<String, MetadataValue>() {
            @Override
            public void accept(String key, MetadataValue metadataValue) {
                if (metadataValue instanceof MetadataStringValue) {
                    MetadataStringValue metadataStringValue = (MetadataStringValue) metadataValue;
                    switch (metadataStringValue.getTransitiveType()) {
                        case PASS_THROUGH:
                            values.put(Utils.encapsulateMetadataKey(transitivePrefix, key), metadataStringValue.getStringValue());
                            break;
                        case DISPOSABLE:
                            values.put(key, metadataStringValue.getStringValue());
                            break;
                        default:
                            break;
                    }
                }
            }
        });
        return values;
    }

}
