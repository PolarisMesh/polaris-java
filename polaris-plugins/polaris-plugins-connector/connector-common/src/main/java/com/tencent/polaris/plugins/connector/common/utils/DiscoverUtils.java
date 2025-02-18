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

package com.tencent.polaris.plugins.connector.common.utils;

import com.google.protobuf.StringValue;
import com.google.protobuf.UInt32Value;
import com.tencent.polaris.api.pojo.ServiceEventKey;
import com.tencent.polaris.specification.api.v1.service.manage.RequestProto;
import com.tencent.polaris.specification.api.v1.service.manage.ResponseProto;
import com.tencent.polaris.specification.api.v1.service.manage.ServiceProto;

import static com.tencent.polaris.specification.api.v1.model.CodeProto.Code.ExecuteSuccess;

/**
 * @author Haotian Zhang
 */
public class DiscoverUtils {

    public static ResponseProto.DiscoverResponse buildEmptyResponse(ServiceEventKey serviceEventKey) {
        ResponseProto.DiscoverResponse.Builder builder = ResponseProto.DiscoverResponse.newBuilder();
        builder.setService(
                ServiceProto.Service.newBuilder().setName(StringValue.newBuilder().setValue(serviceEventKey.getService()).build())
                        .setNamespace(
                                StringValue.newBuilder().setValue(serviceEventKey.getNamespace()).build()));
        builder.setCode(UInt32Value.newBuilder().setValue(ExecuteSuccess.getNumber()).build());
        builder.setType(buildDiscoverResponseType(serviceEventKey.getEventType()));
        return builder.build();
    }

    public static RequestProto.DiscoverRequest.DiscoverRequestType buildDiscoverRequestType(
            ServiceEventKey.EventType type) {
        switch (type) {
            case INSTANCE:
                return RequestProto.DiscoverRequest.DiscoverRequestType.INSTANCE;
            case ROUTING:
                return RequestProto.DiscoverRequest.DiscoverRequestType.ROUTING;
            case RATE_LIMITING:
                return RequestProto.DiscoverRequest.DiscoverRequestType.RATE_LIMIT;
            case CIRCUIT_BREAKING:
                return RequestProto.DiscoverRequest.DiscoverRequestType.CIRCUIT_BREAKER;
            case SERVICE:
                return RequestProto.DiscoverRequest.DiscoverRequestType.SERVICES;
            case FAULT_DETECTING:
                return RequestProto.DiscoverRequest.DiscoverRequestType.FAULT_DETECTOR;
            case LANE_RULE:
                return RequestProto.DiscoverRequest.DiscoverRequestType.LANE;
            case NEARBY_ROUTE_RULE:
                return RequestProto.DiscoverRequest.DiscoverRequestType.NEARBY_ROUTE_RULE;
            case LOSSLESS:
                return RequestProto.DiscoverRequest.DiscoverRequestType.LOSSLESS;
            case BLOCK_ALLOW_RULE:
                return RequestProto.DiscoverRequest.DiscoverRequestType.BLOCK_ALLOW_RULE;
            default:
                return RequestProto.DiscoverRequest.DiscoverRequestType.UNKNOWN;
        }
    }

    public static ResponseProto.DiscoverResponse.DiscoverResponseType buildDiscoverResponseType(
            ServiceEventKey.EventType type) {
        switch (type) {
            case INSTANCE:
                return ResponseProto.DiscoverResponse.DiscoverResponseType.INSTANCE;
            case ROUTING:
                return ResponseProto.DiscoverResponse.DiscoverResponseType.ROUTING;
            case RATE_LIMITING:
                return ResponseProto.DiscoverResponse.DiscoverResponseType.RATE_LIMIT;
            case CIRCUIT_BREAKING:
                return ResponseProto.DiscoverResponse.DiscoverResponseType.CIRCUIT_BREAKER;
            case SERVICE:
                return ResponseProto.DiscoverResponse.DiscoverResponseType.SERVICES;
            case FAULT_DETECTING:
                return ResponseProto.DiscoverResponse.DiscoverResponseType.FAULT_DETECTOR;
            case LANE_RULE:
                return ResponseProto.DiscoverResponse.DiscoverResponseType.LANE;
            case NEARBY_ROUTE_RULE:
                return ResponseProto.DiscoverResponse.DiscoverResponseType.NEARBY_ROUTE_RULE;
            case LOSSLESS:
                return ResponseProto.DiscoverResponse.DiscoverResponseType.LOSSLESS;
            case BLOCK_ALLOW_RULE:
                return ResponseProto.DiscoverResponse.DiscoverResponseType.BLOCK_ALLOW_RULE;
            default:
                return ResponseProto.DiscoverResponse.DiscoverResponseType.UNKNOWN;
        }
    }

    public static ServiceEventKey.EventType buildEventType(ResponseProto.DiscoverResponse.DiscoverResponseType responseType) {
        switch (responseType) {
            case INSTANCE:
                return ServiceEventKey.EventType.INSTANCE;
            case ROUTING:
                return ServiceEventKey.EventType.ROUTING;
            case RATE_LIMIT:
                return ServiceEventKey.EventType.RATE_LIMITING;
            case CIRCUIT_BREAKER:
                return ServiceEventKey.EventType.CIRCUIT_BREAKING;
            case SERVICES:
                return ServiceEventKey.EventType.SERVICE;
            case FAULT_DETECTOR:
                return ServiceEventKey.EventType.FAULT_DETECTING;
            case LANE:
                return ServiceEventKey.EventType.LANE_RULE;
            case NEARBY_ROUTE_RULE:
                return ServiceEventKey.EventType.NEARBY_ROUTE_RULE;
            case LOSSLESS:
                return ServiceEventKey.EventType.LOSSLESS;
            case BLOCK_ALLOW_RULE:
                return ServiceEventKey.EventType.BLOCK_ALLOW_RULE;
            default:
                return ServiceEventKey.EventType.UNKNOWN;
        }
    }
}
