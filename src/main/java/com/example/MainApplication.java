package com.example;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.netty.resources.ConnectionProvider;

import java.net.URISyntaxException;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class MainApplication {

    private static final Logger logger = LoggerFactory.getLogger(MainApplication.class);

    public static void main(String[] args) throws InterruptedException {
        ConnectionProvider fixedConnectionProvider = ConnectionProvider.fixed("reactor-netty" +
            "-connection", 5, 45 * 1000);
        String[] hostList = { "http://crunchify.com", "http://yahoo.com", "http://www.ebay.com",
            "http://google.com",
            "http://www.example.co", "https://paypal.com", "http://bing.com/", "http://techcrunch" +
            ".com/", "http://mashable.com/",
            "http://thenextweb.com/", "http://wordpress.com/", "http://wordpress.org/", "http" +
            "://example.com/", "http://sjsu.edu/",
            "http://ebay.co.uk/", "http://google123.co.uk/", "http://wikipedia.org/", "http://en" +
            ".wikipedia.org" };

        int threadPoolSize = Runtime.getRuntime().availableProcessors();
        logger.debug("Creating thread pool with size : {}", threadPoolSize);
        ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);

        ReactorNettyClient reactorNettyClient =
            ReactorNettyClient.createWithConnectionProvider(fixedConnectionProvider);

        for (int i = 0; i < 50; i++) {
            String url = hostList[i % hostList.length];
            logger.info("Requesting {}", url);
            Runnable worker = new PaginationRunnable(reactorNettyClient, url);
            executor.execute(worker);
        }
        //  long sleep
        Thread.sleep(1000 * 60 * 60);
        executor.shutdown();
        // Wait until all threads are finish
        while (!executor.isTerminated()) {

        }
        logger.info("Everything done");
    }

    static class PaginationRunnable implements Runnable {

        private final ReactorNettyClient reactorNettyClient;
        private final String url;

        PaginationRunnable(ReactorNettyClient reactorNettyClient, String url) {
            this.reactorNettyClient = reactorNettyClient;
            this.url = url;
        }

        @Override
        public void run() {
            getPaginatedResponseUsingExpand(reactorNettyClient)
                .takeUntil(pageResponse -> pageResponse.count > 4)
                .doOnNext(pageResponse -> logger.info("Response is : {}", pageResponse)).subscribeOn(Schedulers.parallel()).subscribe();
        }

        public Flux<PageResponse> getPaginatedResponseUsingExpand(ReactorNettyClient reactorNettyClient) {
            Fetcher fetcher = new Fetcher();
            return fetcher.getResponse(reactorNettyClient, getRequest(url), url).expand
                (pageResponse -> {
                    if (!fetcher.shouldFetchMore()) {
                        logger.debug("Stopping now : counter : {}", fetcher.count);
                        return Flux.empty();
                    }
                    HttpRequest request = getRequest(url);
                    logger.debug("Fetching more for request : {}", url);
                    return fetcher.getResponse(reactorNettyClient, request, url);
                }).limitRate(1).flatMap(httpResponse -> {
                Mono<byte[]> bodyAsByteArray = httpResponse.bodyAsByteArray();
                return bodyAsByteArray.map(bytes -> {
                    String body = new String(bytes);
                    return new PageResponse(UUID.randomUUID().toString(), body,
                        fetcher.count.get());
                });
            });
        }

        public HttpRequest getRequest(String url) {
            byte[] bytes = getBody(url).getBytes();
            Flux<ByteBuf> bodyByteBufFlux = Flux.just(Unpooled.wrappedBuffer(bytes));
            HttpHeaders httpHeaders = getHttpHeaders(bytes);
            try {
                return new HttpRequest(HttpMethod.POST, "https://rel.ink/api/links/", httpHeaders,
                    bodyByteBufFlux);
            } catch (URISyntaxException e) {
                logger.error("Error occurred while parsing uri", e);
                throw new RuntimeException(e);
            }
        }

        public String getBody(String url) {
            return "{\"url\": \"" + url + "\"}";
        }

        public HttpHeaders getHttpHeaders(byte[] bytes) {
            HttpHeaders httpHeaders = new HttpHeaders();
            httpHeaders.set("Content-Type", "application/json; utf-8");
            httpHeaders.set("Accept", "application/json");
            httpHeaders.set("Cache-Control", "no-cache");
            httpHeaders.set("Transfer-Encoding", "chunked");
            httpHeaders.set("Content-Length", String.valueOf(bytes.length));
            return httpHeaders;
        }
    }

    static class Fetcher {

        final AtomicInteger count = new AtomicInteger();

        public boolean shouldFetchMore() {
            return count.get() < 10;
        }

        public Mono<HttpResponse> getResponse(ReactorNettyClient reactorNettyClient,
                                              HttpRequest httpRequest, String url) {
            logger.debug("Getting response for url {}", url);
            count.incrementAndGet();
            return reactorNettyClient.send(httpRequest);
        }
    }

    static class PageResponse {

        private final String id;
        private final String body;
        private final int count;

        public PageResponse(String id, String body, int count) {
            this.id = id;
            this.body = body;
            this.count = count;
        }

        @Override
        public String toString() {
            return "PageResponse{" +
                "id='" + id + '\'' +
                ", body='" + body + '\'' +
                ", count=" + count +
                '}';
        }
    }
}
