// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.example;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelOption;
import io.netty.handler.codec.http.HttpMethod;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.ByteBufFlux;
import reactor.netty.Connection;
import reactor.netty.NettyOutbound;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.client.HttpClientRequest;
import reactor.netty.http.client.HttpClientResponse;
import reactor.netty.resources.ConnectionProvider;

import java.util.Objects;
import java.util.function.BiFunction;

/**
 * HttpClient that is implemented using reactor-netty.
 */
class ReactorNettyClient {

    private static final Logger logger = LoggerFactory.getLogger(ReactorNettyClient.class);

    private HttpClient httpClient;
    private ConnectionProvider connectionProvider;

    private ReactorNettyClient() {
    }

    /**
     * Creates ReactorNettyClient with {@link ConnectionProvider}.
     */
    public static ReactorNettyClient createWithConnectionProvider(ConnectionProvider connectionProvider) {
        ReactorNettyClient reactorNettyClient = new ReactorNettyClient();
        reactorNettyClient.connectionProvider = connectionProvider;
        reactorNettyClient.httpClient = HttpClient.create(connectionProvider);
        reactorNettyClient.configureChannelPipelineHandlers();
        return reactorNettyClient;
    }

    private void configureChannelPipelineHandlers() {
        this.httpClient = this.httpClient.tcpConfiguration(tcpClient -> {

            tcpClient = tcpClient.secure();
            //  By default, keep alive is enabled on http client
            tcpClient = tcpClient.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 45000);

            return tcpClient;
        });
    }

    public Mono<HttpResponse> send(final HttpRequest request) {
        Objects.requireNonNull(request.httpMethod());
        Objects.requireNonNull(request.uri());

        return this.httpClient
            .keepAlive(true)
            .request(HttpMethod.valueOf(request.httpMethod().toString()))
            .uri(request.uri().toString())
            .send(bodySendDelegate(request))
            .responseConnection(responseDelegate(request))
            .single();
    }

    /**
     * Delegate to send the request content.
     *
     * @param restRequest the Rest request contains the body to be sent
     * @return a delegate upon invocation sets the request body in reactor-netty outbound object
     */
    private static BiFunction<HttpClientRequest, NettyOutbound, Publisher<Void>> bodySendDelegate(final HttpRequest restRequest) {
        return (reactorNettyRequest, reactorNettyOutbound) -> {
            for (HttpHeader header : restRequest.headers()) {
                reactorNettyRequest.header(header.name(), header.value());
            }
            if (restRequest.body() != null) {
                return reactorNettyOutbound.send(restRequest.body());
            } else {
                return reactorNettyOutbound;
            }
        };
    }

    /**
     * Delegate to receive response.
     *
     * @param restRequest the Rest request whose response this delegate handles
     * @return a delegate upon invocation setup Rest response object
     */
    private static BiFunction<HttpClientResponse, Connection, Publisher<HttpResponse>> responseDelegate(final HttpRequest restRequest) {
        return (reactorNettyResponse, reactorNettyConnection) ->
            Mono.just(new ReactorNettyHttpResponse(reactorNettyResponse, reactorNettyConnection).withRequest(restRequest));
    }

    private static class ReactorNettyHttpResponse extends HttpResponse {
        private final HttpClientResponse reactorNettyResponse;
        private final Connection reactorNettyConnection;

        ReactorNettyHttpResponse(HttpClientResponse reactorNettyResponse,
                                 Connection reactorNettyConnection) {
            this.reactorNettyResponse = reactorNettyResponse;
            this.reactorNettyConnection = reactorNettyConnection;
        }

        @Override
        public int statusCode() {
            return reactorNettyResponse.status().code();
        }

        @Override
        public String headerValue(String name) {
            return reactorNettyResponse.responseHeaders().get(name);
        }

        @Override
        public HttpHeaders headers() {
            HttpHeaders headers = new HttpHeaders(reactorNettyResponse.responseHeaders().size());
            reactorNettyResponse.responseHeaders().forEach(e -> headers.set(e.getKey(),
                e.getValue()));
            return headers;
        }

        @Override
        public Flux<ByteBuf> body() {
            return bodyIntern();
        }

        @Override
        public Mono<byte[]> bodyAsByteArray() {
            return bodyIntern().aggregate().asByteArray();
        }

        @Override
        public Mono<String> bodyAsString() {
            return bodyIntern().aggregate().asString();
        }

        private ByteBufFlux bodyIntern() {
            return reactorNettyConnection.inbound().receive();
        }
    }
}
