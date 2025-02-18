/*
 * Tencent is pleased to support the open source community by making polaris-java available.
 *
 * Copyright (C) 2021 THL A29 Limited, a Tencent company. All rights reserved.
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

package com.tencent.polaris.factory.config.global;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.tencent.polaris.api.config.global.AdminConfig;
import com.tencent.polaris.api.utils.StringUtils;
import com.tencent.polaris.factory.util.ConfigUtils;

public class AdminConfigImpl implements AdminConfig {

    @JsonProperty
    private String host;

    @JsonProperty
    private Integer port;

    @Override
    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    @Override
    public int getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    @Override
    public void verify() {
        ConfigUtils.validateString(host, "global.admin.host");
        ConfigUtils.validatePositiveInteger(port, "global.admin.port");
    }

    @Override
    public void setDefault(Object defaultObject) {
        if (null != defaultObject) {
            AdminConfig adminConfig = (AdminConfig) defaultObject;
            if (null == port || 0 == port) {
                setPort(adminConfig.getPort());
            }
            if (StringUtils.isBlank(host)) {
                setHost(adminConfig.getHost());
            }
        }
    }

    @Override
    public String toString() {
        return "AdminConfigImpl{" +
                "host='" + host + '\'' +
                ", port=" + port +
                '}';
    }
}
