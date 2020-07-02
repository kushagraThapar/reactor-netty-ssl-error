// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.example;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelOption;
import io.netty.handler.codec.http.HttpMethod;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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

        AtomicReference<ReactorNettyHttpResponse> responseAtomicReference = new AtomicReference<>();

        return this.httpClient
            .keepAlive(true)
            .request(HttpMethod.valueOf(request.httpMethod().toString()))
            .uri(request.uri().toString())
            .send(bodySendDelegate(request))
            .responseConnection((reactorNettyResponse, reactorNettyConnection) -> {
                HttpResponse httpResponse = new ReactorNettyHttpResponse(reactorNettyResponse,
                    reactorNettyConnection).withRequest(request);
                responseAtomicReference.set((ReactorNettyHttpResponse)httpResponse);
                return Mono.just(httpResponse);
            })
            .doOnCancel(() -> {
                logger.info("Cancellation called");
                ReactorNettyHttpResponse reactorNettyHttpResponse = responseAtomicReference.get();
                logger.info("Atomic Reference : {}", reactorNettyHttpResponse);
                if (reactorNettyHttpResponse != null) {
                    reactorNettyHttpResponse.releaseAfterCancel(request.httpMethod());
                }
            })
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

        // 0 - not subscribed, 1 - subscribed, 2 - cancelled, 3 - cancelled via connector (before
        // subscribe)
        private final AtomicInteger state = new AtomicInteger(0);

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
            return bodyIntern()
                .doOnSubscribe(this::updateSubscriptionState)
                .doOnCancel(this::updateCancellationState)
                .map(byteBuf -> {
                    byteBuf.retain();
                    return byteBuf;
                });
        }

        @Override
        public Mono<byte[]> bodyAsByteArray() {
            return bodyIntern().aggregate()
                               .asByteArray()
                               .doOnSubscribe(this::updateSubscriptionState)
                               .doOnCancel(this::updateCancellationState);
        }

        @Override
        public Mono<String> bodyAsString() {
            return bodyIntern().aggregate()
                               .asString()
                               .doOnSubscribe(this::updateSubscriptionState)
                               .doOnCancel(this::updateCancellationState);
        }

        private ByteBufFlux bodyIntern() {
            return reactorNettyConnection.inbound().receive();
        }

        public Connection getReactorNettyConnection() {
            return reactorNettyConnection;
        }

        private void updateSubscriptionState(Subscription subscription) {
            if (this.state.compareAndSet(0, 1)) {
                return;
            }
            // https://github.com/reactor/reactor-netty/issues/503
            // FluxReceive rejects multiple subscribers, but not after a cancel().
            // Subsequent subscribers after cancel() will not be rejected, but will hang instead.
            // So we need to reject once in cancelled state.
            if (this.state.get() == 2) {
                throw new IllegalStateException(
                    "The client response body can only be consumed once.");
            } else if (this.state.get() == 3) {
                throw new IllegalStateException(
                    "The client response body has been released already due to cancellation.");
            }
        }

        private void updateCancellationState() {
            this.state.compareAndSet(1, 2);
        }

        /**
         * Called by {@link ReactorNettyClient} when a cancellation is detected
         * but the content has not been subscribed to. If the subscription never
         * materializes then the content will remain not drained. Or it could still
         * materialize if the cancellation happened very early, or the response
         * reading was delayed for some reason.
         */
        private void releaseAfterCancel(HttpMethod method) {
            logger.info("Releasing after cancel : {}", method);
            if (mayHaveBody(method) && this.state.compareAndSet(0, 3)) {
                logger.info("Releasing body, not yet subscribed");
                if (logger.isDebugEnabled()) {
                    logger.debug("Releasing body, not yet subscribed");
                }
                this.bodyIntern()
                    .doOnNext(byteBuf -> {})
                    .subscribe(byteBuf -> {}, ex -> {});
            }
        }

        private boolean mayHaveBody(HttpMethod method) {
            int code = this.statusCode();
            return !((code >= 100 && code < 200) || code == 204 || code == 205 ||
                method.equals(HttpMethod.HEAD) || headers().getContentLength() == 0);
        }
    }
}
