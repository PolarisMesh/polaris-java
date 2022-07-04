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

package com.tencent.polaris.plugins.stat.prometheus.handler;

import static com.tencent.polaris.plugins.stat.common.model.SystemMetricModel.SystemMetricLabelOrder;
import static com.tencent.polaris.plugins.stat.common.model.SystemMetricModel.SystemMetricName;
import static com.tencent.polaris.plugins.stat.common.model.SystemMetricModel.SystemMetricValue.NULL_VALUE;

import com.tencent.polaris.api.plugin.stat.CircuitBreakGauge;
import com.tencent.polaris.api.plugin.stat.RateLimitGauge;
import com.tencent.polaris.api.plugin.stat.StatInfo;
import com.tencent.polaris.api.pojo.InstanceGauge;
import com.tencent.polaris.logging.LoggerFactory;
import com.tencent.polaris.plugins.stat.common.model.AbstractSignatureStatInfoCollector;
import com.tencent.polaris.plugins.stat.common.model.MetricValueAggregationStrategy;
import com.tencent.polaris.plugins.stat.common.model.MetricValueAggregationStrategyCollections;
import com.tencent.polaris.plugins.stat.common.model.StatInfoCollector;
import com.tencent.polaris.plugins.stat.common.model.StatInfoCollectorContainer;
import com.tencent.polaris.plugins.stat.common.model.StatInfoHandler;
import com.tencent.polaris.plugins.stat.common.model.StatInfoRevisionCollector;
import com.tencent.polaris.plugins.stat.common.model.StatMetric;
import com.tencent.polaris.plugins.stat.common.model.StatRevisionMetric;
import com.tencent.polaris.plugins.stat.common.model.SystemMetricModel;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;

/**
 * 通过向Prometheus PushGateWay推送StatInfo消息来处理StatInfo。
 *
 * @author wallezhang
 */
public class PrometheusHandler implements StatInfoHandler {

    private static final Logger LOG = LoggerFactory.getLogger(PrometheusHandler.class);

    public static final int DEFAULT_INTERVAL_MILLI = 30 * 1000;
    public static final int REVISION_MAX_SCOPE = 2;
    // schedule
    private final AtomicBoolean firstHandle = new AtomicBoolean(false);
    private ScheduledExecutorService scheduledAggregationTask;
    // storage
    private StatInfoCollectorContainer container;
    // push
    private final String callerIp;
    private final CollectorRegistry promRegistry;
    private final Map<String, Gauge> sampleMapping;

    /**
     * 构造函数
     *
     * @param callerIp 调用者Ip
     */
    public PrometheusHandler(String callerIp) {
        this.callerIp = callerIp;
        this.container = new StatInfoCollectorContainer();
        this.sampleMapping = new HashMap<>();
        this.promRegistry = CollectorRegistry.defaultRegistry;
        this.scheduledAggregationTask = Executors.newSingleThreadScheduledExecutor();
        initSampleMapping(MetricValueAggregationStrategyCollections.SERVICE_CALL_STRATEGY,
                SystemMetricLabelOrder.INSTANCE_GAUGE_LABEL_ORDER);
        initSampleMapping(MetricValueAggregationStrategyCollections.RATE_LIMIT_STRATEGY,
                SystemMetricLabelOrder.RATELIMIT_GAUGE_LABEL_ORDER);
        initSampleMapping(MetricValueAggregationStrategyCollections.CIRCUIT_BREAK_STRATEGY,
                SystemMetricLabelOrder.CIRCUIT_BREAKER_LABEL_ORDER);
    }

    private void initSampleMapping(MetricValueAggregationStrategy<?>[] strategies, String[] order) {
        for (MetricValueAggregationStrategy<?> strategy : strategies) {
            Gauge strategyGauge = new Gauge.Builder()
                    .name(strategy.getStrategyName())
                    .help(strategy.getStrategyDescription())
                    .labelNames(order)
                    .create()
                    .register(promRegistry);
            sampleMapping.put(strategy.getStrategyName(), strategyGauge);
        }
    }

    @Override
    public void handle(StatInfo statInfo) {
        if (firstHandle.compareAndSet(false, true)) {
            startScheduleAggregationTask();
        }

        if (null == statInfo) {
            return;
        }

        if (null != statInfo.getRouterGauge()) {
            handleRouterGauge(statInfo.getRouterGauge());
        }

        if (null != statInfo.getCircuitBreakGauge()) {
            handleCircuitBreakGauge(statInfo.getCircuitBreakGauge());
        }

        if (null != statInfo.getRateLimitGauge()) {
            handleRateLimitGauge(statInfo.getRateLimitGauge());
        }
    }

    public void handleRouterGauge(InstanceGauge instanceGauge) {
        if (null != container && null != container.getInsCollector()) {
            container.getInsCollector().collectStatInfo(instanceGauge,
                    convertInsGaugeToLabels(instanceGauge),
                    MetricValueAggregationStrategyCollections.SERVICE_CALL_STRATEGY);
        }
    }

    public void handleRateLimitGauge(RateLimitGauge rateLimitGauge) {
        if (null != container && null != container.getRateLimitCollector()) {
            container.getRateLimitCollector().collectStatInfo(rateLimitGauge,
                    convertRateLimitGaugeToLabels(rateLimitGauge),
                    MetricValueAggregationStrategyCollections.RATE_LIMIT_STRATEGY);
        }
    }

    public void handleCircuitBreakGauge(CircuitBreakGauge circuitBreakGauge) {
        if (null != container && null != container.getCircuitBreakerCollector()) {
            container.getCircuitBreakerCollector().collectStatInfo(circuitBreakGauge,
                    convertCircuitBreakToLabels(circuitBreakGauge),
                    MetricValueAggregationStrategyCollections.CIRCUIT_BREAK_STRATEGY);
        }
    }

    @Override
    public void stopHandle() {
        if (container != null) {
            container = null;
        }

        if (scheduledAggregationTask != null) {
            scheduledAggregationTask.shutdown();
            scheduledAggregationTask = null;
        }
    }

    private void startScheduleAggregationTask() {
        if (null != container && null != scheduledAggregationTask && null != sampleMapping) {
            this.scheduledAggregationTask.scheduleWithFixedDelay(this::doAggregation,
                    DEFAULT_INTERVAL_MILLI,
                    DEFAULT_INTERVAL_MILLI,
                    TimeUnit.MILLISECONDS);
            LOG.info("start schedule metric aggregation task, task interval {}", DEFAULT_INTERVAL_MILLI);
        }
    }

    private void doAggregation() {
        putDataFromContainerInOrder(container.getInsCollector(),
                container.getInsCollector().getCurrentRevision(),
                SystemMetricLabelOrder.INSTANCE_GAUGE_LABEL_ORDER);
        putDataFromContainerInOrder(container.getRateLimitCollector(),
                container.getRateLimitCollector().getCurrentRevision(),
                SystemMetricLabelOrder.RATELIMIT_GAUGE_LABEL_ORDER);
        putDataFromContainerInOrder(container.getCircuitBreakerCollector(),
                0,
                SystemMetricLabelOrder.CIRCUIT_BREAKER_LABEL_ORDER);

        for (StatInfoCollector<?, ? extends StatMetric> s : container.getCollectors()) {
            if (s instanceof StatInfoRevisionCollector<?>) {
                long currentRevision = ((StatInfoRevisionCollector<?>) s).incRevision();
                LOG.debug("RevisionCollector inc current revision to {}", currentRevision);
            }
        }
    }

    private void putDataFromContainerInOrder(AbstractSignatureStatInfoCollector<?, ? extends StatMetric> collector,
            long currentRevision,
            String[] order) {
        Collection<? extends StatMetric> values = collector.getCollectedValues();

        for (StatMetric s : values) {
            Gauge gauge = sampleMapping.get(s.getMetricName());
            if (null != gauge) {
                if (s instanceof StatRevisionMetric) {
                    StatRevisionMetric rs = (StatRevisionMetric) s;
                    if (rs.getRevision() < currentRevision - REVISION_MAX_SCOPE) {
                        // 如果连续两个版本还没有数据，就清除该数据
                        gauge.remove(getOrderedMetricLabelValues(s.getLabels(), order));
                        collector.getMetricContainer().remove(s.getSignature());
                        continue;
                    } else if (rs.getRevision() < currentRevision) {
                        // 如果版本为老版本，则清零数据
                        gauge.remove(getOrderedMetricLabelValues(s.getLabels(), order));
                        Gauge.Child child = gauge.labels(getOrderedMetricLabelValues(s.getLabels(), order));
                        if (null != child) {
                            child.set(0);
                        }
                        continue;
                    }
                }

                Gauge.Child child = gauge.labels(getOrderedMetricLabelValues(s.getLabels(), order));
                if (null != child) {
                    child.set(s.getValue());
                }
            }
        }
    }

    public static String[] getOrderedMetricLabelValues(Map<String, String> labels, String[] orderedKey) {
        String[] orderValue = new String[orderedKey.length];
        for (int i = 0; i < orderedKey.length; i++) {
            orderValue[i] = labels.getOrDefault(orderedKey[i], NULL_VALUE);
        }
        return orderValue;
    }

    protected Map<String, String> convertInsGaugeToLabels(InstanceGauge insGauge) {
        Map<String, String> labels = new HashMap<>();
        for (String labelName : SystemMetricLabelOrder.INSTANCE_GAUGE_LABEL_ORDER) {
            switch (labelName) {
                case SystemMetricName.CALLEE_NAMESPACE:
                    addLabel(labelName, insGauge.getNamespace(), labels);
                    break;
                case SystemMetricModel.SystemMetricName.CALLEE_SERVICE:
                    addLabel(labelName, insGauge.getService(), labels);
                    break;
                case SystemMetricName.CALLEE_METHOD:
                    addLabel(labelName, insGauge.getMethod(), labels);
                    break;
                case SystemMetricName.CALLEE_SUBSET:
                    addLabel(labelName, insGauge.getSubset(), labels);
                    break;
                case SystemMetricModel.SystemMetricName.CALLEE_INSTANCE:
                    addLabel(labelName, buildAddress(insGauge.getHost(), insGauge.getPort()), labels);
                    break;
                case SystemMetricName.CALLEE_RET_CODE:
                    String retCodeStr =
                            null == insGauge.getRetCode() ? null : insGauge.getRetCode().toString();
                    addLabel(labelName, retCodeStr, labels);
                    break;
                case SystemMetricName.CALLER_LABELS:
                    addLabel(labelName, insGauge.getLabels(), labels);
                    break;
                case SystemMetricName.CALLER_NAMESPACE:
                    String namespace =
                            null == insGauge.getCallerService() ? null : insGauge.getCallerService().getNamespace();
                    addLabel(labelName, namespace, labels);
                    break;
                case SystemMetricName.CALLER_SERVICE:
                    String serviceName =
                            null == insGauge.getCallerService() ? null : insGauge.getCallerService().getService();
                    addLabel(labelName, serviceName, labels);
                    break;
                case SystemMetricName.CALLER_IP:
                    addLabel(labelName, callerIp, labels);
                    break;
                default:
            }
        }

        return labels;
    }

    protected Map<String, String> convertRateLimitGaugeToLabels(RateLimitGauge rateLimitGauge) {
        Map<String, String> labels = new HashMap<>();
        for (String labelName : SystemMetricModel.SystemMetricLabelOrder.RATELIMIT_GAUGE_LABEL_ORDER) {
            switch (labelName) {
                case SystemMetricName.CALLEE_NAMESPACE:
                    addLabel(labelName, rateLimitGauge.getNamespace(), labels);
                    break;
                case SystemMetricName.CALLEE_SERVICE:
                    addLabel(labelName, rateLimitGauge.getService(), labels);
                    break;
                case SystemMetricName.CALLEE_METHOD:
                    addLabel(labelName, rateLimitGauge.getMethod(), labels);
                    break;
                case SystemMetricName.CALLER_LABELS:
                    addLabel(labelName, rateLimitGauge.getLabels(), labels);
                    break;
                default:
            }
        }

        return labels;
    }

    protected Map<String, String> convertCircuitBreakToLabels(CircuitBreakGauge gauge) {
        Map<String, String> labels = new HashMap<>();
        for (String labelName : SystemMetricModel.SystemMetricLabelOrder.CIRCUIT_BREAKER_LABEL_ORDER) {
            switch (labelName) {
                case SystemMetricName.CALLEE_NAMESPACE:
                    addLabel(labelName, gauge.getNamespace(), labels);
                    break;
                case SystemMetricName.CALLEE_SERVICE:
                    addLabel(labelName, gauge.getService(), labels);
                    break;
                case SystemMetricName.CALLEE_METHOD:
                    addLabel(labelName, gauge.getMethod(), labels);
                    break;
                case SystemMetricName.CALLEE_SUBSET:
                    addLabel(labelName, gauge.getSubset(), labels);
                    break;
                case SystemMetricName.CALLEE_INSTANCE:
                    addLabel(labelName, buildAddress(gauge.getHost(), gauge.getPort()), labels);
                    break;
                case SystemMetricName.CALLER_NAMESPACE:
                    String namespace =
                            null == gauge.getCallerService() ? null : gauge.getCallerService().getNamespace();
                    addLabel(labelName, namespace, labels);
                    break;
                case SystemMetricName.CALLER_SERVICE:
                    String serviceName =
                            null == gauge.getCallerService() ? null : gauge.getCallerService().getService();
                    addLabel(labelName, serviceName, labels);
                    break;
                case SystemMetricName.CALLER_IP:
                    addLabel(labelName, callerIp, labels);
                    break;
                default:
            }
        }

        return labels;
    }

    private void addLabel(String key, String value, Map<String, String> target) {
        if (null == key) {
            return;
        }

        if (null == value) {
            value = NULL_VALUE;
        }

        target.put(key, value);
    }

    private static String buildAddress(String host, int port) {
        if (null == host) {
            host = "";
        }

        return host + ":" + port;
    }

    protected CollectorRegistry getPromRegistry() {
        return promRegistry;
    }
}
