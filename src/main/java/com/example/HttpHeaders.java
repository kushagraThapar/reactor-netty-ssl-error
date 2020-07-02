// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.example;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;

/**
 * A collection of headers on an HTTP request or response.
 */
public class HttpHeaders implements Iterable<HttpHeader> {
    private final Map<String, HttpHeader> headers;

    /**
     * Create an empty HttpHeaders instance.
     */
    public HttpHeaders() {
        this.headers = new HashMap<>();
    }

    /**
     * Create an HttpHeaders instance with the given size.
     */
    public HttpHeaders(int size) {
        this.headers = new HashMap<>(size);
    }

    /**
     * Set a header.
     *
     * if header with same name already exists then the value will be overwritten.
     * if value is null and header with provided name already exists then it will be removed.
     *
     * @param name the name
     * @param value the value
     * @return this HttpHeaders
     */
    public HttpHeaders set(String name, String value) {
        final String headerKey = name.toLowerCase(Locale.ROOT);
        if (value == null) {
            headers.remove(headerKey);
        } else {
            headers.put(headerKey, new HttpHeader(name, value));
        }
        return this;
    }

    public long getContentLength() {
        HttpHeader httpHeader = headers.get("Content-Length");
        if (httpHeader != null) {
            return Long.parseLong(httpHeader.value());
        }
        return -1;
    }

    @Override
    public Iterator<HttpHeader> iterator() {
        return headers.values().iterator();
    }
}
