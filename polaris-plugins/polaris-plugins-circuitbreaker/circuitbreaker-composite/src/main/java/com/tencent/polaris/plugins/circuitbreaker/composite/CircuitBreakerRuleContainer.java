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

import static com.tencent.polaris.plugins.circuitbreaker.composite.MatchUtils.isWildcardMatcherSingle;
import static com.tencent.polaris.plugins.circuitbreaker.composite.MatchUtils.matchMethod;
import static com.tencent.polaris.plugins.circuitbreaker.composite.MatchUtils.matchService;

import com.tencent.polaris.api.plugin.cache.FlowCache;
import com.tencent.polaris.api.plugin.circuitbreaker.entity.Resource;
import com.tencent.polaris.api.pojo.ServiceEventKey;
import com.tencent.polaris.api.pojo.ServiceEventKey.EventType;
import com.tencent.polaris.api.pojo.ServiceKey;
import com.tencent.polaris.api.pojo.ServiceRule;
import com.tencent.polaris.api.utils.StringUtils;
import com.tencent.polaris.logging.LoggerFactory;
import com.tencent.polaris.specification.api.v1.fault.tolerance.CircuitBreakerProto.CircuitBreaker;
import com.tencent.polaris.specification.api.v1.fault.tolerance.CircuitBreakerProto.CircuitBreakerRule;
import com.tencent.polaris.specification.api.v1.fault.tolerance.CircuitBreakerProto.Level;
import com.tencent.polaris.specification.api.v1.fault.tolerance.CircuitBreakerProto.RuleMatcher;
import com.tencent.polaris.specification.api.v1.fault.tolerance.CircuitBreakerProto.RuleMatcher.DestinationService;
import com.tencent.polaris.specification.api.v1.fault.tolerance.CircuitBreakerProto.RuleMatcher.SourceService;
import com.tencent.polaris.specification.api.v1.fault.tolerance.FaultDetectorProto.FaultDetector;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Pattern;
import org.slf4j.Logger;

public class CircuitBreakerRuleContainer {

    private static final Logger LOG = LoggerFactory.getLogger(CircuitBreakerRuleContainer.class);

    private final Resource resource;

    private final PolarisCircuitBreaker polarisCircuitBreaker;

    private final Function<String, Pattern> regexToPattern;

    public CircuitBreakerRuleContainer(Resource resource, PolarisCircuitBreaker polarisCircuitBreaker) {
        this.resource = resource;
        this.polarisCircuitBreaker = polarisCircuitBreaker;
        this.regexToPattern = regex -> {
            FlowCache flowCache = polarisCircuitBreaker.getExtensions().getFlowCache();
            return flowCache.loadOrStoreCompiledRegex(regex);
        };
        scheduleCircuitBreaker();
    }

    private class PullCircuitBreakerRuleTask implements Runnable {

        @Override
        public void run() {
            synchronized (resource) {
                ServiceEventKey cbEventKey = new ServiceEventKey(resource.getService(),
                        EventType.CIRCUIT_BREAKING);
                ServiceRule circuitBreakRule;
                try {
                    circuitBreakRule = polarisCircuitBreaker.getServiceRuleProvider().getServiceRule(cbEventKey);
                } catch (Throwable t) {
                    LOG.warn("fail to get resource for {}", cbEventKey, t);
                    polarisCircuitBreaker.getPullRulesExecutors()
                            .schedule(new PullCircuitBreakerRuleTask(), 5, TimeUnit.SECONDS);
                    return;
                }
                Map<Resource, ResourceCounters> resourceResourceCounters = polarisCircuitBreaker.getCountersCache()
                        .get(resource.getLevel());
                CircuitBreakerRule circuitBreakerRule = selectRule(resource, circuitBreakRule, regexToPattern);
                if (null != circuitBreakerRule) {
                    ResourceCounters resourceCounters = resourceResourceCounters.get(resource);
                    if (null != resourceCounters) {
                        CircuitBreakerRule currentActiveRule = resourceCounters.getCurrentActiveRule();
                        if (StringUtils.equals(currentActiveRule.getId(), circuitBreakerRule.getId()) && StringUtils
                                .equals(currentActiveRule.getRevision(), circuitBreakerRule.getRevision())) {
                            return;
                        }
                    }
                    resourceResourceCounters.put(resource, new ResourceCounters(resource, circuitBreakerRule,
                            polarisCircuitBreaker.getStateChangeExecutors(), polarisCircuitBreaker));
                    scheduleHealthCheck();
                } else {
                    ResourceCounters oldCounters = resourceResourceCounters.remove(resource);
                    if (null != oldCounters) {
                        // remove the old health check scheduler
                        scheduleHealthCheck();
                    }
                }
            }
        }
    }

    private class PullFaultDetectTask implements Runnable {

        @Override
        public void run() {
            synchronized (resource) {
                LOG.info("start to pull fault detect rule for resource {}", resource);
                ResourceCounters resourceCounters = polarisCircuitBreaker.getCountersCache().get(resource.getLevel())
                        .get(resource);
                boolean faultDetectEnabled = false;
                CircuitBreakerRule currentActiveRule = null;
                if (null != resourceCounters) {
                    currentActiveRule = resourceCounters.getCurrentActiveRule();
                }
                if (null != currentActiveRule && currentActiveRule.getEnable() && currentActiveRule
                        .getFaultDetectConfig().getEnable()) {
                    ServiceEventKey fdEventKey = new ServiceEventKey(resource.getService(),
                            EventType.FAULT_DETECTING);
                    ServiceRule faultDetectRule;
                    try {
                        faultDetectRule = polarisCircuitBreaker.getServiceRuleProvider().getServiceRule(fdEventKey);
                    } catch (Throwable t) {
                        LOG.warn("fail to get resource for {}", fdEventKey, t);
                        polarisCircuitBreaker.getPullRulesExecutors()
                                .schedule(new PullFaultDetectTask(), 5, TimeUnit.SECONDS);
                        return;
                    }
                    FaultDetector faultDetector = selectFaultDetector(faultDetectRule);
                    Map<Resource, ResourceHealthChecker> healthCheckCache = polarisCircuitBreaker.getHealthCheckCache();
                    Map<ServiceKey, Map<Resource, ResourceHealthChecker>> serviceHealthCheckCache = polarisCircuitBreaker
                            .getServiceHealthCheckCache();
                    if (null != faultDetector) {
                        ResourceHealthChecker curChecker = healthCheckCache.get(resource);
                        if (null != curChecker) {
                            FaultDetector currentRule = curChecker.getFaultDetector();
                            if (StringUtils.equals(faultDetector.getRevision(), currentRule.getRevision())) {
                                return;
                            }
                            curChecker.stop();
                        }
                        ResourceHealthChecker newHealthChecker = new ResourceHealthChecker(resource, faultDetector,
                                polarisCircuitBreaker);
                        healthCheckCache.put(resource, newHealthChecker);
                        if (resource.getLevel() != Level.INSTANCE) {
                            ServiceKey svcKey = resource.getService();
                            Map<Resource, ResourceHealthChecker> resourceHealthCheckerMap = serviceHealthCheckCache
                                    .get(svcKey);
                            if (null == resourceHealthCheckerMap) {
                                resourceHealthCheckerMap = new ConcurrentHashMap<>();
                                serviceHealthCheckCache.put(svcKey, resourceHealthCheckerMap);
                            }
                            resourceHealthCheckerMap.put(resource, newHealthChecker);
                        }
                        faultDetectEnabled = true;
                    }
                }
                if (!faultDetectEnabled) {
                    LOG.info("health check for resource {} is disabled, now stop the previous checker", resource);
                    Map<Resource, ResourceHealthChecker> healthCheckCache = polarisCircuitBreaker.getHealthCheckCache();
                    ResourceHealthChecker preChecker = healthCheckCache.remove(resource);
                    if (null != preChecker) {
                        preChecker.stop();
                    }
                    if (resource.getLevel() != Level.INSTANCE) {
                        ServiceKey svcKey = resource.getService();
                        Map<Resource, ResourceHealthChecker> resourceHealthCheckerMap = polarisCircuitBreaker
                                .getServiceHealthCheckCache().get(svcKey);
                        if (null != resourceHealthCheckerMap) {
                            resourceHealthCheckerMap.remove(resource);
                            if (resourceHealthCheckerMap.isEmpty()) {
                                polarisCircuitBreaker.getServiceHealthCheckCache().remove(svcKey);
                            }
                        }
                    }
                }
            }
        }
    }

    private FaultDetector selectFaultDetector(ServiceRule serviceRule) {
        if (null == serviceRule) {
            return null;
        }
        Object rule = serviceRule.getRule();
        if (null == rule) {
            return null;
        }
        return (FaultDetector) rule;
    }

    private static List<CircuitBreakerRule> sortCircuitBreakerRules(List<CircuitBreakerRule> rules) {
        List<CircuitBreakerRule> outRules = new ArrayList<>(rules);
        outRules.sort(new Comparator<CircuitBreakerRule>() {
            @Override
            public int compare(CircuitBreakerRule rule1, CircuitBreakerRule rule2) {
                // 1. compare destination service
                RuleMatcher ruleMatcher1 = rule1.getRuleMatcher();
                String destNamespace1 = ruleMatcher1.getDestination().getNamespace();
                String destService1 = ruleMatcher1.getDestination().getService();
                String destMethod1 = ruleMatcher1.getDestination().getMethod().getValue().getValue();

                RuleMatcher ruleMatcher2 = rule2.getRuleMatcher();
                String destNamespace2 = ruleMatcher2.getDestination().getNamespace();
                String destService2 = ruleMatcher2.getDestination().getService();
                String destMethod2 = ruleMatcher2.getDestination().getMethod().getValue().getValue();

                int svcResult = compareService(destNamespace1, destService1, destNamespace2, destService2);
                if (svcResult != 0) {
                    return svcResult;
                }
                if (rule1.getLevel() == Level.METHOD && rule1.getLevel() == rule2.getLevel()) {
                    int methodResult = compareSingleValue(destMethod1, destMethod2);
                    if (methodResult != 0) {
                        return methodResult;
                    }
                }
                // 2. compare source service
                String srcNamespace1 = ruleMatcher1.getSource().getNamespace();
                String srcService1 = ruleMatcher1.getSource().getService();
                String srcNamespace2 = ruleMatcher2.getSource().getNamespace();
                String srcService2 = ruleMatcher2.getSource().getService();
                return compareService(srcNamespace1, srcService1, srcNamespace2, srcService2);
            }
        });
        return outRules;
    }

    public static int compareSingleValue(String value1, String value2) {
        boolean serviceWildcard1 = isWildcardMatcherSingle(value1);
        boolean serviceWildcard2 = isWildcardMatcherSingle(value2);
        if (serviceWildcard1 && serviceWildcard2) {
            return 0;
        }
        if (serviceWildcard1) {
            // 1 before 2
            return 1;
        }
        if (serviceWildcard2) {
            // 1 before 2
            return -1;
        }
        return value1.compareTo(value2);
    }

    public static int compareService(String namespace1, String service1, String namespace2, String service2) {
        int nsResult = compareSingleValue(namespace1, namespace2);
        if (nsResult != 0) {
            return nsResult;
        }
        return compareSingleValue(service1, service2);
    }

    public static CircuitBreakerRule selectRule(Resource resource, ServiceRule serviceRule,
            Function<String, Pattern> regexToPattern) {
        if (null == serviceRule) {
            return null;
        }
        Object rule = serviceRule.getRule();
        if (null == rule) {
            return null;
        }
        CircuitBreaker circuitBreaker = (CircuitBreaker) rule;
        List<CircuitBreakerRule> rules = circuitBreaker.getRulesList();
        if (rules.isEmpty()) {
            return null;
        }
        List<CircuitBreakerRule> sortedRules = sortCircuitBreakerRules(rules);
        for (CircuitBreakerRule cbRule : sortedRules) {
            if (!cbRule.getEnable()) {
                continue;
            }
            Level level = resource.getLevel();
            if (cbRule.getLevel() != level) {
                continue;
            }
            RuleMatcher ruleMatcher = cbRule.getRuleMatcher();
            DestinationService destination = ruleMatcher.getDestination();
            if (!matchService(resource.getService(), destination.getNamespace(), destination.getService())) {
                continue;
            }
            SourceService source = ruleMatcher.getSource();
            if (!matchService(resource.getCallerService(), source.getNamespace(), source.getService())) {
                continue;
            }
            boolean methodMatched = matchMethod(resource, destination.getMethod(), regexToPattern);
            if (methodMatched) {
                return cbRule;
            }
        }
        return null;
    }

    public void scheduleCircuitBreaker() {
        polarisCircuitBreaker.getPullRulesExecutors()
                .schedule(new PullCircuitBreakerRuleTask(), 50, TimeUnit.MILLISECONDS);
    }

    public void scheduleHealthCheck() {
        polarisCircuitBreaker.getPullRulesExecutors().schedule(
                new PullFaultDetectTask(), 100, TimeUnit.MILLISECONDS);
    }

    public Resource getResource() {
        return resource;
    }
}
