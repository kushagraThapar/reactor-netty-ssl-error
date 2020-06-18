// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.example;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpMethod;
import reactor.core.publisher.Flux;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * The outgoing Http request.
 */
public class HttpRequest {
    private HttpMethod httpMethod;
    private URI uri;
    private HttpHeaders headers;
    private Flux<ByteBuf> body;

    /**
     * Create a new HttpRequest instance.
     *
     * @param httpMethod the HTTP request method
     * @param uri        the target address to send the request to
     */
    public HttpRequest(HttpMethod httpMethod, String uri, HttpHeaders httpHeaders, Flux<ByteBuf> body) throws URISyntaxException {
        this.httpMethod = httpMethod;
        this.uri = new URI(uri);
        this.headers = httpHeaders;
        this.body = body;
    }

    /**
     * Get the request method.
     *
     * @return the request method
     */
    public HttpMethod httpMethod() {
        return httpMethod;
    }

    /**
     * Get the target address.
     *
     * @return the target address
     */
    public URI uri() {
        return uri;
    }

    /**
     * Get the request headers.
     *
     * @return headers to be sent
     */
    public HttpHeaders headers() {
        return headers;
    }

    /**
     * Get the request content.
     *
     * @return the content to be send
     */
    public Flux<ByteBuf> body() {
        return body;
    }

    /**
     * Set request content.
     * <p>
     * Caller must set the Content-Length header to indicate the length of the content,
     * or use Transfer-Encoding: chunked.
     *
     * @param content the request content
     * @return this HttpRequest
     */
    public HttpRequest withBody(Flux<ByteBuf> content) {
        this.body = content;
        return this;
    }
}
