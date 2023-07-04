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

package com.tencent.polaris.factory.config.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.tencent.polaris.api.config.configuration.ConfigFilterConfig;
import com.tencent.polaris.api.config.configuration.CryptoConfig;
import com.tencent.polaris.api.config.verify.Verifier;
import com.tencent.polaris.api.exception.PolarisException;
import com.tencent.polaris.factory.config.plugin.PluginConfigImpl;
import com.tencent.polaris.factory.util.ConfigUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * configfilter 配置
 *
 * @author fabian4
 * @date 2023/6/13
 */
public class ConfigFilterConfigImpl extends PluginConfigImpl implements ConfigFilterConfig {

    @JsonProperty
    private Boolean enable;

    @JsonProperty
    private List<String> chain;

    @Override
    public boolean isEnable() {
        return enable != null && enable;
    }

    @Override
    public List<String> getChain() {
        return this.chain;
    }

    @Override
    public Map<String, Verifier> getPluginConfigs() throws PolarisException {
        Map<String, Verifier> values = new HashMap<>();
        chain.forEach(chain -> {
            CryptoConfig cryptoConfig = super.getPluginConfig(chain, CryptoConfig.class);
            values.put(chain, cryptoConfig);
        });
        return values;
    }

    @Override
    public void verify() {
        ConfigUtils.validateNull(chain, "chain");
    }

    @Override
    public void setDefault(Object defaultObject) {}
}
