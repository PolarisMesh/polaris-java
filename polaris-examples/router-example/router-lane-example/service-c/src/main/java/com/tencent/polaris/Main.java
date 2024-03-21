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

package com.tencent.polaris;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.tencent.polaris.api.config.Configuration;
import com.tencent.polaris.api.core.ProviderAPI;
import com.tencent.polaris.api.rpc.InstanceDeregisterRequest;
import com.tencent.polaris.api.rpc.InstanceRegisterRequest;
import com.tencent.polaris.api.rpc.InstanceRegisterResponse;
import com.tencent.polaris.api.utils.StringUtils;
import com.tencent.polaris.factory.ConfigAPIFactory;
import com.tencent.polaris.factory.api.DiscoveryAPIFactory;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

public class Main {
    private static final String NAMESPACE_DEFAULT = "default";

    private static final String ECHO_SERVICE_NAME = "service-c";

    private static final int TTL = 5;

    public static void main(String[] args) throws Exception {

        CommandLineParser parser = new DefaultParser();
        Options options = new Options();
        options.addOption("lane", "provider lane", true, "lane for instance");

        CommandLine commandLine = parser.parse(options, args);
        String lane = commandLine.getOptionValue("lane");

        String namespace = NAMESPACE_DEFAULT;
        String service = ECHO_SERVICE_NAME;

        Configuration configuration = ConfigAPIFactory.defaultConfig();
        String localHost = getLocalHost(configuration);
        ProviderAPI providerAPI = DiscoveryAPIFactory.createProviderAPIByConfig(configuration);

        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/echo", new EchoServerHandler(lane));

        int localPort = server.getAddress().getPort();
        register(namespace, service, localHost, localPort, lane, providerAPI);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.stop(1);
            deregister(namespace, service, localHost, localPort, providerAPI);
            providerAPI.close();
        }));
        server.start();
    }

    // do the instance register
    private static void register(String namespace, String service, String host, int port, String lane,
                                 ProviderAPI providerAPI) {
        InstanceRegisterRequest registerRequest = new InstanceRegisterRequest();
        registerRequest.setNamespace(namespace);
        registerRequest.setService(service);
        registerRequest.setHost(host);
        registerRequest.setPort(port);
        registerRequest.setTtl(TTL);

        if (StringUtils.isNotBlank(lane)) {
            Map<String, String> metadata = new HashMap<>();
            metadata.put("lane", lane);
            registerRequest.setMetadata(metadata);
        }

        InstanceRegisterResponse registerResp = providerAPI.registerInstance(registerRequest);
        System.out.printf("register instance %s:%d to service %s(%s), id is %s%n",
                host, port, service, namespace, registerResp.getInstanceId());
    }

    // do the instance deregister
    private static void deregister(String namespace, String service, String host, int port,
                                   ProviderAPI providerAPI) {
        InstanceDeregisterRequest deregisterRequest = new InstanceDeregisterRequest();
        deregisterRequest.setNamespace(namespace);
        deregisterRequest.setService(service);
        deregisterRequest.setHost(host);
        deregisterRequest.setPort(port);
        providerAPI.deRegister(deregisterRequest);
        System.out.printf("deregister instance, address is %s:%d%n", host, port);
    }

    private static String getLocalHost(Configuration configuration) throws Exception {
        String serverAddress = configuration.getGlobal().getServerConnector().getAddresses().get(0);
        String[] tokens = serverAddress.split(":");
        try (Socket socket = new Socket(tokens[0], Integer.parseInt(tokens[1]))) {
            return socket.getLocalAddress().getHostAddress();
        }
    }

    private static class EchoServerHandler implements HttpHandler {

        private final String lane;

        public EchoServerHandler(String lane) {
            this.lane = lane;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String selfName = System.getenv("SELF_NAME");
            if (StringUtils.isNotBlank(lane)) {
                selfName = selfName + "-" + lane;
            }
            String ret = "[" + selfName + "]";
            exchange.sendResponseHeaders(200, 0);
            OutputStream os = exchange.getResponseBody();
            os.write(ret.getBytes());
            os.close();
        }
    }
}