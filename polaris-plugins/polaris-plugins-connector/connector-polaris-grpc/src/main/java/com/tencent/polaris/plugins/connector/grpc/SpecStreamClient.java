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

package com.tencent.polaris.plugins.connector.grpc;

import com.google.protobuf.StringValue;
import com.google.protobuf.TextFormat;
import com.tencent.polaris.annonation.JustForTest;
import com.tencent.polaris.api.exception.ErrorCode;
import com.tencent.polaris.api.exception.PolarisException;
import com.tencent.polaris.api.exception.ServerCodes;
import com.tencent.polaris.api.exception.ServerErrorResponseException;
import com.tencent.polaris.api.plugin.server.ServerEvent;
import com.tencent.polaris.api.pojo.ServiceEventKey;
import com.tencent.polaris.api.pojo.ServiceEventKey.EventType;
import com.tencent.polaris.api.pojo.ServiceKey;
import com.tencent.polaris.api.utils.CollectionUtils;
import com.tencent.polaris.api.utils.StringUtils;
import com.tencent.polaris.client.util.NamedThreadFactory;
import com.tencent.polaris.factory.config.global.ServerConnectorConfigImpl;
import com.tencent.polaris.logging.LoggerFactory;
import com.tencent.polaris.plugins.connector.common.ServiceUpdateTask;
import com.tencent.polaris.plugins.connector.common.constant.ServiceUpdateTaskConstant.Type;
import com.tencent.polaris.plugins.connector.common.utils.DiscoverUtils;
import com.tencent.polaris.specification.api.v1.service.manage.PolarisGRPCGrpc;
import com.tencent.polaris.specification.api.v1.service.manage.RequestProto;
import com.tencent.polaris.specification.api.v1.service.manage.ResponseProto;
import com.tencent.polaris.specification.api.v1.service.manage.ServiceProto;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * 用于cluster/healthcheck/heartbeat的服务发现
 *
 * @author Haotian Zhang
 */
public class SpecStreamClient implements StreamObserver<ResponseProto.DiscoverResponse> {


    private static final Logger LOG = LoggerFactory.getLogger(SpecStreamClient.class);

    /**
     * 同步锁
     */
    private final Object clientLock = new Object();

    private ServerConnectorConfigImpl connectorConfig;

    /**
     * 在流中没有结束的任务
     */
    private final Map<ServiceEventKey, SpecTask> pendingTask = new HashMap<>();

    /**
     * 连接是否可用
     */
    private final AtomicBoolean endStream = new AtomicBoolean(false);

    /**
     * GRPC stream客户端
     */
    private final StreamObserver<RequestProto.DiscoverRequest> discoverClient;

    /**
     * 连接对象
     */
    private final Connection connection;

    /**
     * 请求ID
     */
    private final String reqId;

    /**
     * 最近更新时间
     */
    private final AtomicLong lastRecvTimeMs = new AtomicLong(0);

    /**
     * 创建时间
     */
    private final long createTimeMs;

    /**
     * 连接空闲时间
     */
    private final long connectionIdleTimeoutMs;

    /**
     * 清理长期处于 pending 状态的任务
     */
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("clean-expire-pendingtask"));

    private static long PENDING_TASK_EXPIRE_MS = 30_000L;

    /**
     * 构造函数
     *
     * @param connection              连接
     * @param connectionIdleTimeoutMs 连接超时
     * @param serviceUpdateTask       初始更新任务
     */
    public SpecStreamClient(ServerConnectorConfigImpl connectorConfig, Connection connection, long connectionIdleTimeoutMs,
                            ServiceUpdateTask serviceUpdateTask) {
        this.connectorConfig = connectorConfig;
        this.connection = connection;
        this.connectionIdleTimeoutMs = connectionIdleTimeoutMs;
        createTimeMs = System.currentTimeMillis();
        reqId = GrpcUtil.nextGetInstanceReqId();
        PolarisGRPCGrpc.PolarisGRPCStub namingStub = PolarisGRPCGrpc.newStub(connection.getChannel());
        namingStub = GrpcUtil.attachRequestHeader(namingStub, reqId);
        namingStub = GrpcUtil.attachAccessToken(connectorConfig.getToken(), namingStub);
        discoverClient = namingStub.discover(this);
        putPendingTask(serviceUpdateTask);
        executor.schedule(new ExpirePendingTaskCleaner(this, PENDING_TASK_EXPIRE_MS), 5, TimeUnit.SECONDS);
    }

    @JustForTest
    public SpecStreamClient(long pendingTaskExpireMs) {
        this.connection = null;
        this.connectionIdleTimeoutMs = 0;
        this.createTimeMs = System.currentTimeMillis();
        this.discoverClient = null;
        this.reqId = UUID.randomUUID().toString();
        this.executor.schedule(new ExpirePendingTaskCleaner(this, pendingTaskExpireMs), 5, TimeUnit.SECONDS);
    }

    /**
     * 关闭流
     *
     * @param closeSend 是否发送EOF
     */
    public void closeStream(boolean closeSend) {
        executor.shutdownNow();
        boolean endStreamOK = endStream.compareAndSet(false, true);
        if (!endStreamOK) {
            return;
        }
        if (closeSend) {
            LOG.info("[ServerConnector]connection {} start to closeSend", connection.getConnID());
            discoverClient.onCompleted();
        }
        connection.release(GrpcUtil.OP_KEY_DISCOVER);
    }

    private boolean isEndStream() {
        return endStream.get();
    }


    public String getReqId() {
        return reqId;
    }

    /**
     * 发送请求
     *
     * @param serviceUpdateTask 服务更新任务
     */
    public void sendRequest(ServiceUpdateTask serviceUpdateTask) {
        ServiceEventKey serviceEventKey = serviceUpdateTask.getServiceEventKey();
        ServiceProto.Service.Builder builder = ServiceProto.Service.newBuilder();
        builder.setName(StringValue.newBuilder().setValue(serviceEventKey.getServiceKey().getService()).build());
        builder.setNamespace(StringValue.newBuilder().setValue(serviceEventKey.getServiceKey().getNamespace()).build());
        builder.setRevision(
                StringValue.newBuilder().setValue(serviceUpdateTask.getEventHandler().getRevision()).build());

        RequestProto.DiscoverRequest.Builder req = RequestProto.DiscoverRequest.newBuilder();
        req.setType(DiscoverUtils.buildDiscoverRequestType(serviceEventKey.getEventType())); // switch
        req.setService(builder);
        if (serviceUpdateTask.getTaskType() == Type.FIRST) {
            LOG.info("[ServerConnector]send request(id={}) to {} for service {}", reqId, connection.getConnID(),
                    serviceEventKey);
        } else {
            LOG.debug("[ServerConnector]send request(id={}) to {} for service {}", reqId, connection.getConnID(),
                    serviceEventKey);
        }
        discoverClient.onNext(req.build());
    }

    /**
     * 检查该应答是否合法，不合法则走异常流程
     *
     * @param response 服务端应答
     * @return 合法返回true，否则返回false
     */
    private ValidResult validMessage(ResponseProto.DiscoverResponse response) {
        ErrorCode errorCode = ErrorCode.Success;
        if (response.hasCode()) {
            errorCode = ServerCodes.convertServerErrorToRpcError(response.getCode().getValue());
        }
        ServiceProto.Service service = response.getService();
        EventType eventType = DiscoverUtils.buildEventType(response.getType());
        if (!eventType.equals(EventType.SERVICE) && (StringUtils.isEmpty(service.getNamespace().getValue())
                || StringUtils.isEmpty(service.getName().getValue()))) {
            return new ValidResult(null, ErrorCode.INVALID_SERVER_RESPONSE,
                    "service is empty, response text is " + response.toString());
        }
        if (eventType == EventType.UNKNOWN) {
            return new ValidResult(null, ErrorCode.INVALID_SERVER_RESPONSE,
                    "invalid event type " + response.getType());
        }
        ServiceEventKey serviceEventKey = new ServiceEventKey(
                new ServiceKey(service.getNamespace().getValue(), service.getName().getValue()), eventType);
        if (errorCode == ErrorCode.SERVER_ERROR) {
            //返回了500错误
            return new ValidResult(serviceEventKey, errorCode, "invalid event type " + response.getType());
        }

        return new ValidResult(serviceEventKey, ErrorCode.Success, "");
    }

    /**
     * 异常回调
     *
     * @param validResult 异常
     */
    public void exceptionCallback(ValidResult validResult) {
        this.closeStream(false);
        if (validResult.getMessage().contains("EOF")) {
            // print debug log when message contains "EOF" and it is not a normal exception.
            LOG.debug("[ServerConnector]exceptionCallback: errCode {}, info {}, serviceEventKey {}",
                    validResult.getErrorCode(), validResult.getMessage(), validResult.getServiceEventKey());
        } else {
            LOG.error("[ServerConnector]exceptionCallback: errCode {}, info {}, serviceEventKey {}",
                    validResult.getErrorCode(), validResult.getMessage(), validResult.getServiceEventKey());
        }

        //report down
        connection.reportFail(validResult.getErrorCode());
        List<ServiceUpdateTask> notifyTasks = new ArrayList<>();
        synchronized (clientLock) {
            ServiceEventKey serviceEventKey = validResult.getServiceEventKey();
            if (null == serviceEventKey) {
                if (CollectionUtils.isNotEmpty(pendingTask.values())) {
                    notifyTasks.addAll(listPendingTasks());

                    for (SpecTask value : pendingTask.values()) {
                        ServiceUpdateTask task = value.task;
                        PolarisException error = ServerErrorResponseException.build(ErrorCode.NETWORK_ERROR.getCode(),
                                String.format("[ServerConnector]code %s, fail to query service %s from server(%s): %s",
                                        validResult.getErrorCode(), task.getServiceEventKey(),
                                        connection.getConnID(), validResult.getMessage()));
                        task.notifyServerEvent(new ServerEvent(task.getServiceEventKey(), null, error));
                    }

                    pendingTask.clear();
                }
            } else {
                ServiceUpdateTask task = removePendingTask(serviceEventKey);
                if (null != task) {
                    notifyTasks.add(task);
                    PolarisException error = ServerErrorResponseException.build(ErrorCode.NETWORK_ERROR.getCode(),
                            String.format("[ServerConnector]code %s, fail to query service %s from server(%s): %s",
                                    validResult.getErrorCode(), task.getServiceEventKey(),
                                    connection.getConnID(), validResult.getMessage()));
                    task.notifyServerEvent(new ServerEvent(task.getServiceEventKey(), null, error));

                }
            }
        }
        for (ServiceUpdateTask notifyTask : notifyTasks) {
            notifyTask.retry();
        }
    }

    /**
     * 正常回调
     *
     * @param response 应答说
     */
    public void onNext(ResponseProto.DiscoverResponse response) {
        lastRecvTimeMs.set(System.currentTimeMillis());
        ValidResult validResult = validMessage(response);
        if (validResult.errorCode != ErrorCode.Success) {
            exceptionCallback(validResult);
            return;
        }
        ServiceProto.Service service = response.getService();
        ServiceKey serviceKey = new ServiceKey(service.getNamespace().getValue(), service.getName().getValue());
        EventType eventType = DiscoverUtils.buildEventType(response.getType());
        ServiceEventKey serviceEventKey = new ServiceEventKey(serviceKey, eventType);
        ServiceUpdateTask updateTask;
        synchronized (clientLock) {
            updateTask = removePendingTask(serviceEventKey);
        }
        if (null == updateTask) {
            LOG.warn("[ServerConnector]callback not found for:{}", TextFormat.shortDebugString(service));
            return;
        }
        if (updateTask.getTaskType() == Type.FIRST) {
            LOG.info("[ServerConnector]request(id={}) receive response for {}", getReqId(), serviceEventKey);
        } else {
            LOG.debug("[ServerConnector]request(id={}) receive response for {}", getReqId(), serviceEventKey);
        }
        boolean svcDeleted = updateTask.notifyServerEvent(new ServerEvent(serviceEventKey, response, null));
        if (!svcDeleted) {
            updateTask.addUpdateTaskSet();
        }
    }

    @Override
    public void onError(Throwable throwable) {
        exceptionCallback(new ValidResult(null, ErrorCode.NETWORK_ERROR,
                String.format("stream %s received error from server(%s), error is %s", getReqId(),
                        connection.getConnID().toString(), throwable.getMessage())));
    }

    @Override
    public void onCompleted() {
        exceptionCallback(new ValidResult(null, ErrorCode.NETWORK_ERROR,
                String.format("stream %s EOF by server(%s)", getReqId(), connection.getConnID().toString())));
    }

    /**
     * 同步清理过期的流对象
     */
    public void syncCloseExpireStream() {
        synchronized (clientLock) {
            closeExpireStream();
        }
    }

    private boolean closeExpireStream() {
        //检查是否过期
        long lastRecvMs = lastRecvTimeMs.get();
        long nowMs = System.currentTimeMillis();
        long connIdleTime;
        if (lastRecvMs == 0) {
            //如果没有收到过消息，就通过创建时间来判断
            connIdleTime = nowMs - createTimeMs;
        } else {
            connIdleTime = nowMs - lastRecvMs;
        }
        if (connIdleTime >= connectionIdleTimeoutMs) {
            //连接已经过期
            closeStream(true);
            return true;
        }
        return false;
    }

    /**
     * checkAvailable
     *
     * @param serviceUpdateTask 更新任务
     * @return 是否可用
     */
    public boolean checkAvailable(ServiceUpdateTask serviceUpdateTask) {
        if (isEndStream()) {
            return false;
        }
        if (!Connection.isAvailableConnection(connection)) {
            return false;
        }
        synchronized (clientLock) {
            if (isEndStream()) {
                return false;
            }
            if (closeExpireStream()) {
                return false;
            }
            ServiceEventKey serviceEventKey = serviceUpdateTask.getServiceEventKey();
            SpecTask lastUpdateTask = pendingTask.get(serviceEventKey);
            if (null != lastUpdateTask) {
                LOG.warn("[ServerConnector]pending task {} has been overwritten", lastUpdateTask);
            }
            putPendingTask(serviceUpdateTask);
        }
        return true;
    }

    private List<ServiceUpdateTask> listPendingTasks() {
        return pendingTask.values().stream().map(specTask -> specTask.task).collect(Collectors.toList());
    }

    public void putPendingTask(ServiceUpdateTask serviceUpdateTask) {
        LOG.debug("Put " + serviceUpdateTask.getServiceEventKey() + "to pending task map. reqId: " + reqId);
        pendingTask.put(serviceUpdateTask.getServiceEventKey(), new SpecTask(serviceUpdateTask));
    }

    public ServiceUpdateTask removePendingTask(ServiceEventKey serviceEventKey) {
        LOG.debug("Remove " + serviceEventKey + "from pending task map. reqId: " + reqId);
        SpecTask task = pendingTask.remove(serviceEventKey);
        if (Objects.isNull(task)) {
            return null;
        }
        return task.task;
    }

    private static class ValidResult {

        final ServiceEventKey serviceEventKey;
        final ErrorCode errorCode;
        final String message;

        public ValidResult(ServiceEventKey serviceEventKey, ErrorCode errorCode, String message) {
            this.serviceEventKey = serviceEventKey;
            this.errorCode = errorCode;
            this.message = message;
        }

        public ServiceEventKey getServiceEventKey() {
            return serviceEventKey;
        }

        public ErrorCode getErrorCode() {
            return errorCode;
        }

        public String getMessage() {
            return message;
        }
    }

    private static class SpecTask {
        private final ServiceUpdateTask task;

        private final long submitTimeMs;

        private SpecTask(ServiceUpdateTask task) {
            this.task = task;
            this.submitTimeMs = System.currentTimeMillis();
        }
    }

    private static class ExpirePendingTaskCleaner implements Runnable {

        private final SpecStreamClient client;

        private final long pendingTaskExpireMs;

        private ExpirePendingTaskCleaner(SpecStreamClient client, long pendingTaskExpireMs) {
            this.client = client;
            this.pendingTaskExpireMs = pendingTaskExpireMs;
        }

        @Override
        public void run() {
            try {
                realCheck();
            } finally {
                client.executor.schedule(this, 5, TimeUnit.SECONDS);
            }
        }

        private void realCheck() {
            long current = System.currentTimeMillis();
            synchronized (client.clientLock) {
                Map<ServiceEventKey, ServiceUpdateTask> waitRetry = new HashMap<>();
                for (Map.Entry<ServiceEventKey, SpecTask> entry : client.pendingTask.entrySet()) {
                    SpecTask specTask = entry.getValue();
                    if (current - specTask.submitTimeMs > pendingTaskExpireMs) {
                        waitRetry.put(entry.getKey(), specTask.task);
                    }
                }

                waitRetry.forEach((serviceEventKey, serviceUpdateTask) -> {
                    LOG.info("[ServerConnector] retry pending task {}, because it's long time to running", serviceEventKey);
                    client.removePendingTask(serviceEventKey);
                    serviceUpdateTask.retry();
                });
            }
        }
    }
}
