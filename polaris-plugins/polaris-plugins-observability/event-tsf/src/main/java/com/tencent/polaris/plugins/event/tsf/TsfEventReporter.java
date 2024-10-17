/*
 * Tencent is pleased to support the open source community by making Polaris available.
 *
 * Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
 *
 *  Licensed under the BSD 3-Clause License (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.tencent.polaris.plugins.event.tsf;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.tencent.polaris.api.config.global.EventReporterConfig;
import com.tencent.polaris.api.config.plugin.DefaultPlugins;
import com.tencent.polaris.api.exception.PolarisException;
import com.tencent.polaris.api.plugin.PluginType;
import com.tencent.polaris.api.plugin.common.InitContext;
import com.tencent.polaris.api.plugin.common.PluginTypes;
import com.tencent.polaris.api.plugin.compose.Extensions;
import com.tencent.polaris.api.plugin.event.EventReporter;
import com.tencent.polaris.api.plugin.event.FlowEvent;
import com.tencent.polaris.api.pojo.ServiceEventKey;
import com.tencent.polaris.api.utils.CollectionUtils;
import com.tencent.polaris.api.utils.StringUtils;
import com.tencent.polaris.api.utils.ThreadPoolUtils;
import com.tencent.polaris.client.util.NamedThreadFactory;
import com.tencent.polaris.logging.LoggerFactory;
import com.tencent.polaris.plugins.event.tsf.v1.EventResponse;
import com.tencent.polaris.plugins.event.tsf.v1.TsfEventData;
import com.tencent.polaris.plugins.event.tsf.v1.TsfEventDataPair;
import com.tencent.polaris.plugins.event.tsf.v1.TsfGenericEvent;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static com.tencent.polaris.api.exception.ErrorCode.SERVER_USER_ERROR;
import static com.tencent.polaris.api.plugin.event.tsf.TsfEventDataConstants.*;

/**
 * @author Haotian Zhang
 */
public class TsfEventReporter implements EventReporter {

    private static final Logger LOG = LoggerFactory.getLogger(TsfEventReporter.class);

    private static final Gson gson = new GsonBuilder().disableHtmlEscaping().create();

    private final BlockingQueue<TsfEventData> v1EventQueue = new LinkedBlockingQueue<>(QUEUE_THRESHOLD);

    private volatile boolean init = true;

    private TsfEventReporterConfig tsfEventReporterConfig;

    private URI v1EventUri;

    private final ScheduledExecutorService eventV1Executors = new ScheduledThreadPoolExecutor(1,
            new NamedThreadFactory("event-tsf-v1"));

    @Override
    public boolean reportEvent(FlowEvent flowEvent) {
        if (flowEvent.getEventType().equals(ServiceEventKey.EventType.CIRCUIT_BREAKING)) {
            return reportV1Event(flowEvent);
        }
        return true;
    }

    private boolean reportV1Event(FlowEvent flowEvent) {
        try {
            if (v1EventUri == null) {
                LOG.warn("build event request url fail, can not sent event.");
                return false;
            }

            TsfEventData eventData = new TsfEventData();
            eventData.setOccurTime(flowEvent.getTimestamp().getEpochSecond());
            eventData.setEventName(TsfEventDataUtils.convertEventName(flowEvent));
            Byte status = TsfEventDataUtils.convertStatus(flowEvent);
            if (status == null || status == -1) {
                return true;
            }
            eventData.setStatus(status);

            List<TsfEventDataPair> dimensions = new ArrayList<>();
            dimensions.add(new TsfEventDataPair(APP_ID_KEY, tsfEventReporterConfig.getAppId()));
            dimensions.add(new TsfEventDataPair(NAMESPACE_ID_KEY, tsfEventReporterConfig.getTsfNamespaceId()));
            dimensions.add(new TsfEventDataPair(SERVICE_NAME, tsfEventReporterConfig.getServiceName()));
            eventData.setDimensions(dimensions);

            List<TsfEventDataPair> additionalMsg = new ArrayList<>();
            additionalMsg.add(new TsfEventDataPair(UPSTREAM_SERVICE_KEY, tsfEventReporterConfig.getServiceName()));
            additionalMsg.add(new TsfEventDataPair(UPSTREAM_NAMESPACE_ID_KEY, tsfEventReporterConfig.getTsfNamespaceId()));
            additionalMsg.add(new TsfEventDataPair(DOWNSTREAM_SERVICE_KEY, flowEvent.getService()));
            additionalMsg.add(new TsfEventDataPair(DOWNSTREAM_NAMESPACE_ID_KEY, flowEvent.getNamespace()));
            String isolationObject = "";
            for (Map.Entry<String, String> entry : flowEvent.getAdditionalParams().entrySet()) {
                additionalMsg.add(new TsfEventDataPair(entry.getKey(), entry.getValue()));
                if (StringUtils.equals(entry.getKey(), ISOLATION_OBJECT_KEY)) {
                    isolationObject = entry.getValue();
                }
            }
            eventData.setAdditionalMsg(additionalMsg);

            if (StringUtils.isNotBlank(isolationObject)) {
                eventData.setInstanceId(tsfEventReporterConfig.getInstanceId() + "#" + isolationObject);
            } else {
                eventData.setInstanceId(tsfEventReporterConfig.getInstanceId());
            }

            // 如果满了就抛出异常
            try {
                v1EventQueue.add(eventData);
            } catch (Exception e) {
                LOG.warn("eventQueue is full. Log this event and drop it. {}", flowEvent);
            }
            return true;
        } catch (Throwable throwable) {
            LOG.warn("Failed to send event to TSF. {}", flowEvent, throwable);
            return false;
        }
    }

    @Override
    public String getName() {
        return DefaultPlugins.TSF_EVENT_REPORTER_TYPE;
    }

    @Override
    public PluginType getType() {
        return PluginTypes.EVENT_REPORTER.getBaseType();
    }

    @Override
    public void init(InitContext ctx) throws PolarisException {
        EventReporterConfig eventReporterConfig = ctx.getConfig().getGlobal().getEventReporter();
        if (eventReporterConfig != null && CollectionUtils.isNotEmpty(eventReporterConfig.getReporters())) {
            for (String reporter : eventReporterConfig.getReporters()) {
                if (StringUtils.equals(getName(), reporter)) {
                    this.tsfEventReporterConfig = ctx.getConfig().getGlobal().getEventReporter()
                            .getPluginConfig(getName(), TsfEventReporterConfig.class);
                    init = false;
                }
            }
        }
    }

    @Override
    public void postContextInit(Extensions ctx) throws PolarisException {
        if (!init) {
            synchronized (this) {
                if (!init) {
                    init = true;
                    try {
                        String v1Path = String.format("/v1/event/%s/%s",
                                URLEncoder.encode(tsfEventReporterConfig.getServiceName(), "UTF-8"),
                                URLEncoder.encode(tsfEventReporterConfig.getInstanceId(), "UTF-8"));
                        v1EventUri = new URIBuilder()
                                .setScheme("http")
                                .setHost(tsfEventReporterConfig.getEventMasterIp())
                                .setPort(tsfEventReporterConfig.getEventMasterPort())
                                .setPath(v1Path)
                                .setParameter("token", tsfEventReporterConfig.getToken())
                                .build();
                        LOG.info("Tsf event reporter init with v1 uri: {}", v1EventUri);
                        eventV1Executors.scheduleWithFixedDelay(new TsfEventV1Task(), 1000, 1000, TimeUnit.MILLISECONDS);
                        LOG.info("Tsf event reporter starts reporting task.");
                    } catch (URISyntaxException | UnsupportedEncodingException e) {
                        LOG.error("Build event request url fail.", e);
                    }
                }
            }
        }
    }

    @Override
    public void destroy() {
        ThreadPoolUtils.waitAndStopThreadPools(new ExecutorService[]{eventV1Executors});
    }

    class TsfEventV1Task implements Runnable {

        @Override
        public void run() {
            try {
                // 每次把eventQueue发空结束
                while (!v1EventQueue.isEmpty()) {
                    List<TsfEventData> eventDataList = new ArrayList<>();
                    TsfGenericEvent genericEvent = new TsfGenericEvent();

                    v1EventQueue.drainTo(eventDataList, MAX_BATCH_SIZE);
                    genericEvent.setEventData(eventDataList);
                    genericEvent.setAppId(tsfEventReporterConfig.getAppId());
                    genericEvent.setRegion(tsfEventReporterConfig.getRegion());

                    postV1Event(genericEvent);
                }
            } catch (Throwable e) {
                LOG.warn("Tsf v1 event reporter task fail.", e);
            }
        }
    }

    private void postV1Event(TsfGenericEvent genericEvent) {
        StringEntity postBody = null;
        RequestConfig config = RequestConfig.custom().setConnectTimeout(2000).setConnectionRequestTimeout(10000).setSocketTimeout(10000).build();
        try (CloseableHttpClient httpClient = HttpClientBuilder.create().setDefaultRequestConfig(config).build()) {
            HttpPost httpPost = new HttpPost(v1EventUri);
            postBody = new StringEntity(gson.toJson(genericEvent));
            httpPost.setEntity(postBody);
            httpPost.setHeader("Content-Type", "application/json");
            HttpResponse httpResponse;

            httpResponse = httpClient.execute(httpPost);
            String resultString = EntityUtils.toString(httpResponse.getEntity(), "utf-8");
            EventResponse response = gson.fromJson(resultString, EventResponse.class);

            if (response.getRetCode() != 0) {
                throw new RuntimeException("Report v1 event failed. Response = [" + resultString + "].");
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("postV1Event body:{}", postBody);
            }
            LOG.info("Report v1 event To TSF event-center Success. Response is : {}", resultString);
        } catch (Exception e) {
            String message = String.format("Report v1 event to event-master failed, postBody:%s.", postBody);
            throw new PolarisException(SERVER_USER_ERROR, message, e);
        }
    }
}