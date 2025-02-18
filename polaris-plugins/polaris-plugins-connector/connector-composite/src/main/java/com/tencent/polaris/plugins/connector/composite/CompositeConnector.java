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

package com.tencent.polaris.plugins.connector.composite;

import com.tencent.polaris.api.config.consumer.ZeroProtectionConfig;
import com.tencent.polaris.api.config.plugin.DefaultPlugins;
import com.tencent.polaris.api.exception.ErrorCode;
import com.tencent.polaris.api.exception.PolarisException;
import com.tencent.polaris.api.plugin.PluginType;
import com.tencent.polaris.api.plugin.common.InitContext;
import com.tencent.polaris.api.plugin.common.PluginTypes;
import com.tencent.polaris.api.plugin.compose.Extensions;
import com.tencent.polaris.api.plugin.server.*;
import com.tencent.polaris.api.pojo.ServiceEventKey;
import com.tencent.polaris.api.utils.CollectionUtils;
import com.tencent.polaris.client.pojo.ServiceRuleByProto;
import com.tencent.polaris.client.util.NamedThreadFactory;
import com.tencent.polaris.factory.config.global.ServerConnectorConfigImpl;
import com.tencent.polaris.logging.LoggerFactory;
import com.tencent.polaris.plugins.connector.common.DestroyableServerConnector;
import com.tencent.polaris.plugins.connector.common.ServiceUpdateTask;
import com.tencent.polaris.plugins.connector.common.constant.ServiceUpdateTaskConstant;
import com.tencent.polaris.plugins.connector.common.constant.ServiceUpdateTaskConstant.Type;
import com.tencent.polaris.plugins.connector.common.utils.DiscoverUtils;
import com.tencent.polaris.plugins.connector.composite.zero.TestConnectivityTask;
import com.tencent.polaris.plugins.connector.composite.zero.TestConnectivityTaskManager;
import com.tencent.polaris.specification.api.v1.service.manage.ResponseProto;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;

/**
 * An implement of {@link ServerConnector} to connect to Multiple Naming Server.It provides methods to manage resources
 * relate to a service:
 * <ol>
 * <li>registerEventHandler/deRegisterEventHandler to subscribe instance/config for a service.
 * <li>registerInstance/deregisterInstance to register/deregister an instance.
 * <li>heartbeat to send heartbeat manually.
 * </ol>
 *
 * @author Haotian Zhang
 */
public class CompositeConnector extends DestroyableServerConnector {

    private static final Logger LOG = LoggerFactory.getLogger(CompositeConnector.class);

    /**
     * Collection of server connector.
     */
    private List<DestroyableServerConnector> serverConnectors;
    /**
     * If server connector initialized.
     */
    private boolean initialized = false;

    private ZeroProtectionConfig zeroProtectionConfig;
    /**
     * Thread pool for sending request to discovery server.
     */
    private ScheduledThreadPoolExecutor sendDiscoverExecutor;
    /**
     * Thread pool for updating service information.
     */
    private ScheduledThreadPoolExecutor updateServiceExecutor;

    private TestConnectivityTaskManager testConnectivityTaskManager;

    @Override
    public String getName() {
        return DefaultPlugins.SERVER_CONNECTOR_COMPOSITE;
    }

    @Override
    public String getId() {
        return DefaultPlugins.SERVER_CONNECTOR_COMPOSITE;
    }

    @Override
    public boolean isRegisterEnable() {
        return true;
    }

    @Override
    public boolean isDiscoveryEnable() {
        return true;
    }

    @Override
    public boolean isReportServiceContractEnable() {
        return true;
    }

    @Override
    public PluginType getType() {
        return PluginTypes.SERVER_CONNECTOR.getBaseType();
    }

    public List<DestroyableServerConnector> getServerConnectors() {
        return serverConnectors;
    }

    @Override
    public void init(InitContext ctx) throws PolarisException {
        if (getName().equals(ctx.getValueContext().getServerConnectorProtocol())) {
            if (CollectionUtils.isEmpty(serverConnectors)) {
                serverConnectors = new ArrayList<>();
            }
            List<ServerConnectorConfigImpl> serverConnectorConfigs = ctx.getConfig().getGlobal().getServerConnectors();
            for (ServerConnectorConfigImpl serverConnectorConfig : serverConnectorConfigs) {
                DestroyableServerConnector serverConnector = (DestroyableServerConnector) ctx.getPlugins()
                        .getPlugin(PluginTypes.SERVER_CONNECTOR.getBaseType(), serverConnectorConfig.getProtocol());
                serverConnector.init(ctx);
                serverConnectors.add(serverConnector);
            }
            sendDiscoverExecutor = new ScheduledThreadPoolExecutor(1,
                    new NamedThreadFactory(getName() + "-send-discovery"), new CallerRunsPolicy());
            sendDiscoverExecutor.setMaximumPoolSize(1);
            updateServiceExecutor = new ScheduledThreadPoolExecutor(1,
                    new NamedThreadFactory(getName() + "-update-service"));
            updateServiceExecutor.setMaximumPoolSize(1);
            testConnectivityTaskManager = new TestConnectivityTaskManager(ctx);
            zeroProtectionConfig = ctx.getConfig().getConsumer().getZeroProtection();
            initialized = true;
        }
    }

    @Override
    public void postContextInit(Extensions ctx) throws PolarisException {
        if (initialized) {
            this.updateServiceExecutor.scheduleWithFixedDelay(new UpdateServiceTask(), TASK_RETRY_INTERVAL_MS,
                    TASK_RETRY_INTERVAL_MS, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void registerServiceHandler(ServiceEventHandler handler) throws PolarisException {
        checkDestroyed();
        ServiceEventKey serviceEventKey = handler.getServiceEventKey();
        if (!checkEventSupported(serviceEventKey.getEventType())) {
            LOG.info("[CompositeConnector] not supported event type for {}", serviceEventKey);
            handler.getEventHandler()
                    .onEventUpdate(new ServerEvent(serviceEventKey, DiscoverUtils.buildEmptyResponse(serviceEventKey), null));
            return;
        }
        ServiceUpdateTask serviceUpdateTask = new CompositeServiceUpdateTask(handler, this);
        submitServiceHandler(serviceUpdateTask, 0);
    }

    @Override
    public boolean checkEventSupported(ServiceEventKey.EventType eventType) throws PolarisException {
        checkDestroyed();
        boolean supported = true;
        for (DestroyableServerConnector sc : serverConnectors) {
            if (!sc.checkEventSupported(eventType)) {
                supported = false;
            }
        }
        return supported;
    }

    @Override
    public void deRegisterServiceHandler(ServiceEventKey eventKey) throws PolarisException {
        checkDestroyed();
        ServiceUpdateTask serviceUpdateTask = updateTaskSet.get(eventKey);
        if (null != serviceUpdateTask) {
            boolean result = serviceUpdateTask.setType(Type.LONG_RUNNING, Type.TERMINATED);
            LOG.info("[ServerConnector]success to deRegister updateServiceTask {}, result is {}", eventKey, result);
        }
    }

    @Override
    public CommonProviderResponse registerInstance(CommonProviderRequest req, Map<String, String> customHeader)
            throws PolarisException {
        checkDestroyed();
        CommonProviderResponse response = null;
        CommonProviderResponse extendResponse = null;
        for (DestroyableServerConnector sc : serverConnectors) {
            CommonProviderResponse temp = sc.registerInstance(req, customHeader);
            if (DefaultPlugins.SERVER_CONNECTOR_GRPC.equals(sc.getName())) {
                response = temp;
            } else {
                if (null == extendResponse) {
                    extendResponse = temp;
                }
            }
        }
        if (null == response) {
            response = extendResponse;
        }
        if (null == response) {
            throw new PolarisException(ErrorCode.INTERNAL_ERROR, "No one server can be registered.");
        }
        return response;
    }

    @Override
    public void deregisterInstance(CommonProviderRequest req) throws PolarisException {
        checkDestroyed();
        for (DestroyableServerConnector sc : serverConnectors) {
            sc.deregisterInstance(req);
        }
    }

    @Override
    public void heartbeat(CommonProviderRequest req) throws PolarisException {
        checkDestroyed();
        for (DestroyableServerConnector sc : serverConnectors) {
            sc.heartbeat(req);
        }
    }

    @Override
    public ReportClientResponse reportClient(ReportClientRequest req) throws PolarisException {
        checkDestroyed();
        ReportClientResponse response = null;
        for (DestroyableServerConnector sc : serverConnectors) {
            ReportClientResponse temp = sc.reportClient(req);
            if (DefaultPlugins.SERVER_CONNECTOR_GRPC.equals(sc.getName())) {
                response = temp;
            }
        }
        return response;
    }

    @Override
    public ReportServiceContractResponse reportServiceContract(ReportServiceContractRequest req) throws PolarisException {
        checkDestroyed();
        ReportServiceContractResponse response = null;
        for (DestroyableServerConnector sc : serverConnectors) {
            ReportServiceContractResponse temp = sc.reportServiceContract(req);
            if (DefaultPlugins.SERVER_CONNECTOR_GRPC.equals(sc.getName())) {
                response = temp;
            }
        }
        return response;
    }

    @Override
    public ServiceRuleByProto getServiceContract(CommonServiceContractRequest req) throws PolarisException {
        checkDestroyed();
        ServiceRuleByProto response = null;
        for (DestroyableServerConnector sc : serverConnectors) {
            ServiceRuleByProto temp = sc.getServiceContract(req);
            if (DefaultPlugins.SERVER_CONNECTOR_GRPC.equals(sc.getName())) {
                response = temp;
            }
        }
        return response;
    }

    @Override
    public void updateServers(ServiceEventKey svcEventKey) {
        for (DestroyableServerConnector sc : serverConnectors) {
            sc.updateServers(svcEventKey);
        }
    }

    @Override
    public boolean isInitialized() {
        return initialized;
    }

    @Override
    protected void submitServiceHandler(ServiceUpdateTask updateTask, long delayMs) {
        LOG.debug("[CompositeServerConnector]task for service {} has been scheduled discover", updateTask);
        if (updateTask.setStatus(ServiceUpdateTaskConstant.Status.READY, ServiceUpdateTaskConstant.Status.RUNNING)) {
            sendDiscoverExecutor.schedule(updateTask, delayMs, TimeUnit.MILLISECONDS);
        }
    }

    protected boolean submitTestConnectivityTask(ServiceUpdateTask updateTask,
                                                 ResponseProto.DiscoverResponse discoverResponse) {
        if (updateTask instanceof CompositeServiceUpdateTask && isZeroProtectionEnabled() && isNeedTestConnectivity()) {
            LOG.debug("[CompositeServerConnector]task for service {} has been scheduled test connectivity.",
                    updateTask.getServiceEventKey());
            return testConnectivityTaskManager.submitTask(new TestConnectivityTask((CompositeServiceUpdateTask) updateTask,
                    discoverResponse, zeroProtectionConfig));
        }
        return false;
    }

    public boolean isZeroProtectionEnabled() {
        return zeroProtectionConfig.isEnable();
    }

    public boolean isNeedTestConnectivity() {
        return zeroProtectionConfig.isNeedTestConnectivity();
    }
}
