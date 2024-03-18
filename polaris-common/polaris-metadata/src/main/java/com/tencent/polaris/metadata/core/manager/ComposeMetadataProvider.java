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

package com.tencent.polaris.metadata.core.manager;

import com.tencent.polaris.metadata.core.MetadataType;

import java.util.ArrayList;
import java.util.List;

public class ComposeMetadataProvider implements MetadataProvider {

    private final List<MetadataProvider> metadataProviders;

    private final List<String> transitivePrefixes = new ArrayList<>();

    public ComposeMetadataProvider(List<String> prefixes, List<MetadataProvider> metadataProviders) {
        assert null != metadataProviders;
        for (String prefix : prefixes) {
            if (null != prefix && !prefix.isEmpty()) {
                transitivePrefixes.add(prefix);
            }
        }
        this.metadataProviders = metadataProviders;
    }

    @Override
    public String getStringValue(MetadataType metadataType, String key) {
        // 先获取透传标签
        if (!transitivePrefixes.isEmpty()) {
            for (String prefix : transitivePrefixes) {
                String value = getRawStringValue(metadataType, Utils.encapsulateMetadataKey(prefix, key));
                if (null != value) {
                    return value;
                }
            }
        }
        // 透传标签获取失败，则获取原始标签
        return getRawStringValue(metadataType, key);
    }

    @Override
    public String getMapValue(MetadataType metadataType, String key, String mapKey) {
        // 先获取透传标签
        if (!transitivePrefixes.isEmpty()) {
            for (String prefix : transitivePrefixes) {
                String value = getRawMapValue(metadataType, key, Utils.encapsulateMetadataKey(prefix, mapKey));
                if (null != value) {
                    return value;
                }
            }
        }
        // 透传标签获取失败，则获取原始标签
        return getRawMapValue(metadataType, key, mapKey);
    }

    private String getRawStringValue(MetadataType metadataType, String key) {
        for (MetadataProvider metadataProvider : metadataProviders) {
            String value = metadataProvider.getStringValue(metadataType, key);
            if (null != value) {
                return value;
            }
        }
        return null;
    }

    private String getRawMapValue(MetadataType metadataType, String key, String mapKey) {
        for (MetadataProvider metadataProvider : metadataProviders) {
            String value = metadataProvider.getMapValue(metadataType, key, mapKey);
            if (null != value) {
                return value;
            }
        }
        return null;
    }


}
