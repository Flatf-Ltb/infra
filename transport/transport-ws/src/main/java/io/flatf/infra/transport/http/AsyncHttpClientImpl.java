package io.flatf.infra.transport.http;

import org.asynchttpclient.AsyncHandler;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.ClientStats;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Request;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.Response;
import org.asynchttpclient.SignatureCalculator;

import java.io.IOException;
import java.util.function.Predicate;

import static org.asynchttpclient.Dsl.asyncHttpClient;

public enum AsyncHttpClientImpl implements AsyncHttpClient {

    INSTANCE;

    private final org.asynchttpclient.AsyncHttpClient client = asyncHttpClient();

    @Override
    public boolean isClosed() {
        return client.isClosed();
    }

    @Override
    public AsyncHttpClient setSignatureCalculator(SignatureCalculator signatureCalculator) {
        return client.setSignatureCalculator(signatureCalculator);
    }

    @Override
    public BoundRequestBuilder prepare(String method, String url) {
        return client.prepare(method, url);
    }

    @Override
    public BoundRequestBuilder prepareGet(String url) {
        return client.prepareGet(url);
    }

    @Override
    public BoundRequestBuilder prepareConnect(String url) {
        return client.prepareConnect(url);
    }

    @Override
    public BoundRequestBuilder prepareOptions(String url) {
        return client.prepareOptions(url);
    }

    @Override
    public BoundRequestBuilder prepareHead(String url) {
        return client.prepareHead(url);
    }

    @Override
    public BoundRequestBuilder preparePost(String url) {
        return client.preparePost(url);
    }

    @Override
    public BoundRequestBuilder preparePut(String url) {
        return client.preparePut(url);
    }

    @Override
    public BoundRequestBuilder prepareDelete(String url) {
        return client.prepareDelete(url);
    }

    @Override
    public BoundRequestBuilder preparePatch(String url) {
        return client.preparePatch(url);
    }

    @Override
    public BoundRequestBuilder prepareTrace(String url) {
        return client.prepareTrace(url);
    }

    @Override
    public BoundRequestBuilder prepareRequest(Request request) {
        return client.prepareRequest(request);
    }

    @Override
    public BoundRequestBuilder prepareRequest(RequestBuilder requestBuilder) {
        return client.prepareRequest(requestBuilder);
    }

    @Override
    public <T> ListenableFuture<T> executeRequest(Request request,
                                                  AsyncHandler<T> handler) {
        return client.executeRequest(request, handler);
    }

    @Override
    public <T> ListenableFuture<T> executeRequest(RequestBuilder requestBuilder,
                                                  AsyncHandler<T> handler) {
        return client.executeRequest(requestBuilder, handler);
    }

    @Override
    public ListenableFuture<Response> executeRequest(Request request) {
        return client.executeRequest(request);
    }

    @Override
    public ListenableFuture<Response> executeRequest(RequestBuilder requestBuilder) {
        return client.executeRequest(requestBuilder);
    }

    @Override
    public ClientStats getClientStats() {
        return client.getClientStats();
    }

    @Override
    public void flushChannelPoolPartitions(Predicate<Object> predicate) {
        client.flushChannelPoolPartitions(predicate);
    }

    @Override
    public AsyncHttpClientConfig getConfig() {
        return client.getConfig();
    }

    @Override
    public void close() throws IOException {
        client.close();
    }

}
