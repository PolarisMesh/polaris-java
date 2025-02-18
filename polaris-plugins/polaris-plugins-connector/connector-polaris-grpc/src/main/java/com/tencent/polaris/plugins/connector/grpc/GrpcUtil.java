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

import com.tencent.polaris.api.exception.ErrorCode;
import com.tencent.polaris.api.exception.PolarisException;
import com.tencent.polaris.api.exception.ServerCodes;
import com.tencent.polaris.api.utils.MapUtils;
import com.tencent.polaris.api.utils.StringUtils;
import com.tencent.polaris.specification.api.v1.service.manage.ResponseProto;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.MetadataUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

/**
 * GrpcUtil
 *
 * @author vickliu
 * @date
 */
public class GrpcUtil {

    public static final String OP_KEY_REGISTER_INSTANCE = "RegisterInstance";

    public static final String OP_KEY_DEREGISTER_INSTANCE = "DeregisterInstance";

    public static final String OP_KEY_INSTANCE_HEARTBEAT = "InstanceHeartbeat";

    public static final String OP_KEY_DISCOVER = "Discover";

    public static final String OP_KEY_REPORT_CLIENT = "ReportClient";

    public static final String OP_KEY_CHECK_COMPATIBLE = "CheckCompatible";

    public static final String OP_KEY_REPORT_SERVICE_CONTRACT = "ReportServiceContract";

    /**
     * 请求ID的key
     */
    private static final Metadata.Key<String> KEY_REQUEST_ID =
            Metadata.Key.of("request-id", Metadata.ASCII_STRING_MARSHALLER);

    /**
     * 实例注册的请求ID前缀
     */
    private static final String REQ_ID_PREFIX_REGISTERINSTANCE = "1";

    /**
     * 实例反注册的请求ID前缀
     */
    private static final String REQ_ID_PREFIX_DEREGISTERINSTANCE = "2";

    /**
     * 实例心跳的请求ID前缀
     */
    private static final String REQ_ID_PREFIX_HEARTBEAT = "3";

    /**
     * 获取实例的请求ID前缀
     */
    private static final String REQ_ID_PREFIX_GETINSTANCES = "4";

    /**
     * Prefix of request of reporting service contract.
     */
    private static final String REQ_ID_PREFIX_REPORTSERVICECONTRACT = "5";

    /**
     * 实例发现ID发生器
     */
    private static final AtomicLong SEED_INSTANCE_REQ_ID = new AtomicLong();

    /**
     * 实例注册ID发生器
     */
    private static final AtomicLong SEED_REGISTER_REQ_ID = new AtomicLong();

    /**
     * 实例反注册ID发生器
     */
    private static final AtomicLong SEED_DEREGISTER_REQ_ID = new AtomicLong();

    /**
     * 心跳上报ID发生器
     */
    private static final AtomicLong SEED_HEARTBEAT_REQ_ID = new AtomicLong();

    /**
     * Request ID generator of reporting service contract.
     */
    private static final AtomicLong SEED_REPORT_SERVICE_CONTRACT_REQ_ID = new AtomicLong();

    /**
     * 获取下一个实例请求ID
     *
     * @return string
     */
    public static String nextGetInstanceReqId() {
        return String.format("%s%d", REQ_ID_PREFIX_GETINSTANCES, SEED_INSTANCE_REQ_ID.incrementAndGet());
    }

    /**
     * 获取下一个注册请求ID
     *
     * @return string
     */
    public static String nextInstanceRegisterReqId() {
        return String.format("%s%d", REQ_ID_PREFIX_REGISTERINSTANCE, SEED_REGISTER_REQ_ID.incrementAndGet());
    }

    /**
     * 获取下一个反注册请求ID
     *
     * @return string
     */
    public static String nextInstanceDeRegisterReqId() {
        return String.format("%s%d", REQ_ID_PREFIX_DEREGISTERINSTANCE, SEED_DEREGISTER_REQ_ID.incrementAndGet());
    }

    /**
     * 获取下一个心跳请求ID
     *
     * @return string
     */
    public static String nextHeartbeatReqId() {
        return String.format("%s%d", REQ_ID_PREFIX_HEARTBEAT, SEED_HEARTBEAT_REQ_ID.incrementAndGet());
    }

    /**
     * Request ID of reporting service contract.
     *
     * @return string
     */
    public static String nextReportServiceContractReqId() {
        return String.format("%s%d", REQ_ID_PREFIX_REPORTSERVICECONTRACT,
                SEED_REPORT_SERVICE_CONTRACT_REQ_ID.incrementAndGet());
    }


    /**
     * 为GRPC请求添加请求头部
     *
     * @param <T>    桩类型
     * @param stub   请求桩
     * @param nextID 请求ID
     */
    public static <T extends AbstractStub<T>> T attachRequestHeader(T stub, String nextID) {
        Metadata extraHeaders = new Metadata();
        extraHeaders.put(KEY_REQUEST_ID, nextID);
        return stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(extraHeaders));
    }

    /**
     * 为GRPC请求添加自定义请求头部信息
     *
     * @param stub         请求桩
     * @param customHeader 自定义header
     * @param <T>          桩类型
     */
    public static <T extends AbstractStub<T>> T attachRequestHeader(T stub, Map<String, String> customHeader) {
        if (MapUtils.isEmpty(customHeader)) {
            return stub;
        }
        Metadata customMetadata = new Metadata();
        for (Entry<String, String> header : customHeader.entrySet()) {
            customMetadata.put(Metadata.Key.of(header.getKey(), Metadata.ASCII_STRING_MARSHALLER), header.getValue());
        }
        return stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(customMetadata));
    }

    public static <T extends AbstractStub<T>> T attachAccessToken(String token, T stub) {
        if (StringUtils.isBlank(token)) {
            return stub;
        }
        return attachRequestHeader(stub, new HashMap<String, String>() {
            {
                put("X-Polaris-Token", token);
            }
        });
    }

    public static void checkResponse(ResponseProto.Response response) throws PolarisException {
        if (!response.hasCode()) {
            return;
        }
        int respCode = response.getCode().getValue();
        if (respCode == ServerCodes.EXECUTE_SUCCESS || respCode == ServerCodes.EXISTED_RESOURCE || respCode == ServerCodes.NO_NEED_UPDATE) {
            return;
        }
        String info = response.getInfo().getValue();
        PolarisException exception = new PolarisException(ErrorCode.SERVER_USER_ERROR,
                String.format("server error %d: %s", respCode, info));
        exception.setServerErrCode(respCode);
        throw exception;
    }

    public static void checkGrpcException(Throwable t) throws PolarisException {
        if (t instanceof StatusRuntimeException) {
            StatusRuntimeException grpcEx = (StatusRuntimeException) t;
            Status.Code code = grpcEx.getStatus().getCode();
            switch (code) {
                case UNAVAILABLE:
                case DEADLINE_EXCEEDED:
                case ABORTED:
                    return;
            }
            // 如果是服务端未实现
            throw new PolarisException(ErrorCode.SERVER_ERROR, grpcEx.getMessage());
        }
    }
}
