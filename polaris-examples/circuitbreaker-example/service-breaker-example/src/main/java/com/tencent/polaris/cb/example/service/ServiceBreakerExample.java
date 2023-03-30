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

package com.tencent.polaris.cb.example.service;

import com.sun.net.httpserver.HttpServer;
import com.tencent.polaris.api.core.ConsumerAPI;
import com.tencent.polaris.api.pojo.ServiceKey;
import com.tencent.polaris.api.rpc.ServiceCallResult;
import com.tencent.polaris.cb.example.common.HealthServer;
import com.tencent.polaris.circuitbreak.api.CircuitBreakAPI;
import com.tencent.polaris.circuitbreak.api.FunctionalDecorator;
import com.tencent.polaris.circuitbreak.api.pojo.FunctionalDecoratorRequest;
import com.tencent.polaris.circuitbreak.api.pojo.ResultToErrorCode;
import com.tencent.polaris.circuitbreak.client.api.DefaultCircuitBreakAPI;
import com.tencent.polaris.circuitbreak.client.exception.CallAbortedException;
import com.tencent.polaris.circuitbreak.factory.CircuitBreakAPIFactory;
import com.tencent.polaris.factory.api.DiscoveryAPIFactory;
import java.net.InetSocketAddress;
import java.util.function.Consumer;

public class ServiceBreakerExample {

    private static final int PORT_NORMAL = 10883;

    private static final int PORT_ABNORMAL = 10884;

    private static final String NAMESPACE = "test";

    private static final String SERVICE = "svcCbService";

    private static final String LOCAL_HOST = "127.0.0.1";

    private static class Condition {

        final boolean success;
        final int count;

        public Condition(boolean success, int count) {
            this.success = success;
            this.count = count;
        }
    }

    private static void createHttpServers() throws Exception {
        HttpServer normalServer = HttpServer.create(new InetSocketAddress(PORT_NORMAL), 0);
        System.out.println("Service cb normal server listen port is " + PORT_NORMAL);
        normalServer.createContext("/health", new HealthServer(true));
        HttpServer abnormalServer = HttpServer.create(new InetSocketAddress(PORT_ABNORMAL), 0);
        System.out.println("Instance cb abnormal server listen port is " + PORT_ABNORMAL);
        abnormalServer.createContext("/health", new HealthServer(false));
        Thread normalStartThread = new Thread(new Runnable() {
            @Override
            public void run() {
                normalServer.start();
            }
        });
        normalStartThread.setDaemon(true);
        Thread abnormalStartThread = new Thread(new Runnable() {
            @Override
            public void run() {
                abnormalServer.start();
            }
        });
        abnormalStartThread.setDaemon(true);
        normalStartThread.start();
        abnormalStartThread.start();
        Thread.sleep(5000);
    }

    public static void main(String[] args) throws Exception {
        createHttpServers();
        int[] ports = new int[]{PORT_NORMAL, PORT_ABNORMAL};
        CircuitBreakAPI circuitBreakAPI = CircuitBreakAPIFactory.createCircuitBreakAPI();
        ConsumerAPI consumerAPI = DiscoveryAPIFactory
                .createConsumerAPIByContext(((DefaultCircuitBreakAPI) circuitBreakAPI).getSDKContext());
        FunctionalDecoratorRequest makeDecoratorRequest = new FunctionalDecoratorRequest(
                new ServiceKey(NAMESPACE, SERVICE), "");
        makeDecoratorRequest.setResultToErrorCode(new ResultToErrorCode() {
            @Override
            public int onSuccess(Object value) {
                return 200;
            }

            @Override
            public int onError(Throwable throwable) {
                return 500;
            }
        });
        FunctionalDecorator decorator = circuitBreakAPI.makeFunctionalDecorator(makeDecoratorRequest);
        Consumer<Condition> integerConsumer = decorator.decorateConsumer(new Consumer<Condition>() {
            @Override
            public void accept(Condition condition) {
                if (!condition.success) {
                    if (condition.count % 2 == 0) {
                        throw new IllegalArgumentException("value divide 2 is zero");
                    }
                }
                System.out.println("invoke success");
            }
        });
        boolean success = false;
        int afterCount = 20;
        for (int i = 0; i < 500; i++) {
            boolean hasError = false;
            try {
                integerConsumer.accept(new Condition(success, i + 1));
                afterCount--;
                if (afterCount == 0) {
                    success = false;
                }
            } catch (Exception e) {
                hasError = true;
                System.out.println(e.getMessage());
                if (e instanceof CallAbortedException) {
                    success = true;
                    afterCount = 20;
                }
            } finally {
                // report to active health check
                ServiceCallResult serviceCallResult = new ServiceCallResult();
                serviceCallResult.setNamespace(NAMESPACE);
                serviceCallResult.setService(SERVICE);
                serviceCallResult.setHost(LOCAL_HOST);
                serviceCallResult.setPort(ports[i % 2]);
                serviceCallResult.setProtocol("http");
                serviceCallResult.setRetCode(hasError ? 500 : 200);
                serviceCallResult.setDelay(10);
                consumerAPI.updateServiceCallResult(serviceCallResult);
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

