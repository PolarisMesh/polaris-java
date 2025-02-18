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

package com.tencent.polaris.plugins.connector.consul;

import com.ecwid.consul.v1.health.model.HealthService;
import com.tencent.polaris.api.utils.StringUtils;
import com.tencent.polaris.logging.LoggerFactory;
import org.slf4j.Logger;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.tencent.polaris.api.config.plugin.DefaultPlugins.SERVER_CONNECTOR_CONSUL;
import static com.tencent.polaris.plugins.connector.common.constant.ConnectorConstant.SERVER_CONNECTOR_TYPE;

/**
 * Copy from spring-cloud-consul-discovery.
 * {@link org.springframework.cloud.consul.discovery.ConsulServerUtils}
 *
 * @author Spencer Gibb
 * @author Semenkov Alexey
 */
public class ConsulServerUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ConsulServerUtils.class);

    public static String findHost(HealthService healthService) {
        HealthService.Service service = healthService.getService();
        HealthService.Node node = healthService.getNode();

        if (StringUtils.isNotBlank(service.getAddress())) {
            return fixIPv6Address(service.getAddress());
        } else if (StringUtils.isNotBlank(node.getAddress())) {
            return fixIPv6Address(node.getAddress());
        }
        return node.getNode();
    }

    public static String fixIPv6Address(String address) {
        try {
            InetAddress inetAdr = InetAddress.getByName(address);
            if (inetAdr instanceof Inet6Address) {
                return "[" + inetAdr.getHostName() + "]";
            }
            return address;
        } catch (UnknownHostException e) {
            LOG.debug("Not InetAddress: " + address + " , resolved as is.");
            return address;
        }
    }

    public static Map<String, String> getMetadata(HealthService healthService) {
        Map<String, String> metadata = getMetadata(healthService.getService().getTags());
        Map<String, String> serviceMeta = healthService.getService().getMeta();
        if (serviceMeta == null || serviceMeta.size() == 0) {
            return metadata;
        }
        for (Map.Entry<String, String> entry : serviceMeta.entrySet()) {
            if (!StringUtils.isEmpty(entry.getValue()) && !StringUtils.isEmpty(entry.getKey())) {
                metadata.put(entry.getKey(), entry.getValue());
            }
        }
        metadata.put(SERVER_CONNECTOR_TYPE, SERVER_CONNECTOR_CONSUL);
        return metadata;
    }

    public static Map<String, String> getMetadata(List<String> tags) {
        LinkedHashMap<String, String> metadata = new LinkedHashMap<>();
        if (tags != null) {
            for (String tag : tags) {
                String[] parts = StringUtils.delimitedListToStringArray(tag, "=");
                switch (parts.length) {
                    case 0:
                        break;
                    case 1:
                        metadata.put(parts[0], parts[0]);
                        break;
                    case 2:
                        metadata.put(parts[0], parts[1]);
                        break;
                    default:
                        String[] end = Arrays.copyOfRange(parts, 1, parts.length);
                        metadata.put(parts[0], StringUtils.arrayToDelimitedString(end, "="));
                        break;
                }

            }
        }

        return metadata;
    }
}
