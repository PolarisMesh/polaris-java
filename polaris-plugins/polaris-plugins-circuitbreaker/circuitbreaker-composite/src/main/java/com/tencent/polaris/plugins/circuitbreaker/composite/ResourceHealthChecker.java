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

package com.tencent.polaris.plugins.circuitbreaker.composite;

import static com.tencent.polaris.logging.LoggingConsts.LOGGING_HEALTHCHECK_EVENT;
import static com.tencent.polaris.plugins.circuitbreaker.composite.CircuitBreakerRuleContainer.compareService;
import static com.tencent.polaris.plugins.circuitbreaker.composite.CircuitBreakerRuleContainer.compareSingleValue;
import static com.tencent.polaris.plugins.circuitbreaker.composite.MatchUtils.matchMethod;
import static com.tencent.polaris.plugins.circuitbreaker.composite.MatchUtils.matchService;

import com.tencent.polaris.api.plugin.cache.FlowCache;
import com.tencent.polaris.api.plugin.circuitbreaker.ResourceStat;
import com.tencent.polaris.api.plugin.circuitbreaker.entity.InstanceResource;
import com.tencent.polaris.api.plugin.circuitbreaker.entity.Resource;
import com.tencent.polaris.api.plugin.detect.HealthChecker;
import com.tencent.polaris.api.pojo.DefaultInstance;
import com.tencent.polaris.api.pojo.DetectResult;
import com.tencent.polaris.api.pojo.Instance;
import com.tencent.polaris.api.utils.RuleUtils;
import com.tencent.polaris.client.pojo.Node;
import com.tencent.polaris.logging.LoggerFactory;
import com.tencent.polaris.specification.api.v1.fault.tolerance.CircuitBreakerProto.Level;
import com.tencent.polaris.specification.api.v1.fault.tolerance.FaultDetectorProto.FaultDetectRule;
import com.tencent.polaris.specification.api.v1.fault.tolerance.FaultDetectorProto.FaultDetectRule.DestinationService;
import com.tencent.polaris.specification.api.v1.fault.tolerance.FaultDetectorProto.FaultDetectRule.Protocol;
import com.tencent.polaris.specification.api.v1.fault.tolerance.FaultDetectorProto.FaultDetector;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.regex.Pattern;
import org.slf4j.Logger;

public class ResourceHealthChecker {

    private static final Logger HC_EVENT_LOG = LoggerFactory.getLogger(LOGGING_HEALTHCHECK_EVENT);

    private static final Logger LOG = LoggerFactory.getLogger(ResourceHealthChecker.class);

    private static final int DEFAULT_CHECK_INTERVAL = 10;

    private final Resource resource;

    private final FaultDetector faultDetector;

    private final ScheduledExecutorService checkScheduler;

    private final AtomicBoolean stopped = new AtomicBoolean(false);

    private final Map<String, HealthChecker> healthCheckers;

    private final PolarisCircuitBreaker polarisCircuitBreaker;

    private final Function<String, Pattern> regexToPattern;

    private final List<ScheduledFuture<?>> futures = new ArrayList<>();

    private final Map<Node, ProtocolInstance> instances = new ConcurrentHashMap<>();

    public ResourceHealthChecker(Resource resource, FaultDetector faultDetector,
            PolarisCircuitBreaker polarisCircuitBreaker) {
        this.resource = resource;
        this.faultDetector = faultDetector;
        this.regexToPattern = regex -> {
            FlowCache flowCache = polarisCircuitBreaker.getExtensions().getFlowCache();
            return flowCache.loadOrStoreCompiledRegex(regex);
        };
        this.checkScheduler = polarisCircuitBreaker.getHealthCheckExecutors();
        this.healthCheckers = polarisCircuitBreaker.getHealthCheckers();
        this.polarisCircuitBreaker = polarisCircuitBreaker;
        if (resource instanceof InstanceResource) {
            addInstance((InstanceResource) resource);
        }
        start();
    }

    public void addInstance(InstanceResource instanceResource) {
        if (!instances.containsKey(instanceResource.getNode())) {
            instances.put(instanceResource.getNode(),
                    new ProtocolInstance(HealthCheckUtils.parseProtocol(instanceResource.getProtocol()),
                            instanceResource));
        }
    }

    private static List<FaultDetectRule> sortFaultDetectRules(List<FaultDetectRule> rules) {
        List<FaultDetectRule> outRules = new ArrayList<>(rules);
        outRules.sort(new Comparator<FaultDetectRule>() {
            @Override
            public int compare(FaultDetectRule rule1, FaultDetectRule rule2) {
                // 1. compare destination service
                DestinationService targetService1 = rule1.getTargetService();
                String destNamespace1 = targetService1.getNamespace();
                String destService1 = targetService1.getService();
                String destMethod1 = targetService1.getMethod().getValue().getValue();

                DestinationService targetService2 = rule2.getTargetService();
                String destNamespace2 = targetService2.getNamespace();
                String destService2 = targetService2.getService();
                String destMethod2 = targetService2.getMethod().getValue().getValue();

                int svcResult = compareService(destNamespace1, destService1, destNamespace2, destService2);
                if (svcResult != 0) {
                    return svcResult;
                }
                return compareSingleValue(destMethod1, destMethod2);
            }
        });
        return outRules;
    }

    public static Map<String, FaultDetectRule> selectFaultDetectRules(Resource resource,
            FaultDetector faultDetector, Function<String, Pattern> regexToPattern) {
        List<FaultDetectRule> sortedRules = sortFaultDetectRules(faultDetector.getRulesList());
        Map<String, FaultDetectRule> out = new HashMap<>();
        for (FaultDetectRule sortedRule : sortedRules) {
            DestinationService targetService = sortedRule.getTargetService();
            if (!matchService(resource.getService(), targetService.getNamespace(), targetService.getService())) {
                continue;
            }
            if (resource.getLevel() == Level.METHOD) {
                if (!matchMethod(resource, targetService.getMethod(), regexToPattern)) {
                    continue;
                }
            } else {
                // only match empty method rules
                if (!RuleUtils.isMatchAllValue(targetService.getMethod())) {
                    continue;
                }
            }
            if (!out.containsKey(sortedRule.getProtocol().name())) {
                out.put(sortedRule.getProtocol().name(), sortedRule);
            }
        }
        return out;
    }

    private Instance createDefaultInstance(String host, int port) {
        DefaultInstance instance = new DefaultInstance();
        instance.setHost(host);
        instance.setPort(port);
        return instance;
    }

    private Runnable createCheckTask(Protocol protocol, FaultDetectRule faultDetectRule) {
        return () -> {
            if (stopped.get()) {
                return;
            }
            checkResource(protocol, faultDetectRule);
        };
    }

    private void checkResource(Protocol protocol, FaultDetectRule faultDetectRule) {
        int port = faultDetectRule.getPort();
        if (port > 0) {
            Set<String> hosts = new HashSet<>();
            for (Node instance : instances.keySet()) {
                if (!hosts.contains(instance.getHost())) {
                    hosts.add(instance.getHost());
                    doCheck(createDefaultInstance(instance.getHost(), port), protocol, faultDetectRule);
                }
            }
        } else {
            for (Map.Entry<Node, ProtocolInstance> entry : instances.entrySet()) {
                if (protocol == entry.getValue().getProtocol()) {
                    InstanceResource instance = entry.getValue().getInstanceResource();
                    doCheck(createDefaultInstance(instance.getHost(), instance.getPort()), protocol, faultDetectRule);
                }
            }
        }
    }

    private void start() {
        Map<String, FaultDetectRule> protocol2Rules = selectFaultDetectRules(resource, faultDetector, regexToPattern);
        for (Map.Entry<String, FaultDetectRule> entry : protocol2Rules.entrySet()) {
            FaultDetectRule faultDetectRule = entry.getValue();
            Runnable checkTask = createCheckTask(Protocol.valueOf(entry.getKey()), entry.getValue());
            int interval = DEFAULT_CHECK_INTERVAL;
            if (faultDetectRule.getInterval() > 0) {
                interval = faultDetectRule.getInterval();
            }
            LOG.info("schedule task: protocol {}, interval {}, rule {}", entry.getKey(), interval,
                    faultDetectRule.getName());
            ScheduledFuture<?> future = checkScheduler
                    .scheduleWithFixedDelay(checkTask, interval, interval, TimeUnit.SECONDS);
            futures.add(future);
        }
    }

    private void doCheck(Instance instance, Protocol protocol, FaultDetectRule faultDetectRule) {
        HealthChecker healthChecker = healthCheckers.get(protocol.name().toLowerCase());
        if (null == healthChecker) {
            HC_EVENT_LOG
                    .info("plugin not found, skip health check for instance {}:{}, resource {}, protocol {}",
                            instance.getHost(), instance.getPort(), resource, protocol);
            return;
        }
        DetectResult detectResult = healthChecker.detectInstance(instance, faultDetectRule);
        ResourceStat resourceStat = new ResourceStat(resource, detectResult.getStatusCode(), detectResult.getDelay(),
                detectResult.getRetStatus());
        HC_EVENT_LOG
                .info("health check for instance {}:{}, resource {}, protocol {}, result: code {}, delay {}ms, status {}",
                        instance.getHost(), instance.getPort(), resource, protocol, detectResult.getStatusCode(),
                        detectResult.getDelay(), detectResult.getRetStatus());
        polarisCircuitBreaker.report(resourceStat);
    }

    public void stop() {
        LOG.info("health checker for resource {} has stopped", resource);
        stopped.set(true);
        for (ScheduledFuture<?> future : futures) {
            future.cancel(true);
        }
    }

    public FaultDetector getFaultDetector() {
        return faultDetector;
    }

    private static class ProtocolInstance {

        final Protocol protocol;

        final InstanceResource instanceResource;

        ProtocolInstance(
                Protocol protocol, InstanceResource instanceResource) {
            this.protocol = protocol;
            this.instanceResource = instanceResource;
        }

        Protocol getProtocol() {
            return protocol;
        }

        InstanceResource getInstanceResource() {
            return instanceResource;
        }
    }
}
