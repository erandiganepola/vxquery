/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.vxquery.rest;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.HttpClientUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.vxquery.app.util.RestUtils;
import org.apache.vxquery.rest.request.QueryRequest;
import org.apache.vxquery.rest.request.QueryResultRequest;
import org.apache.vxquery.rest.response.AsyncQueryResponse;
import org.apache.vxquery.rest.response.QueryResultResponse;
import org.apache.vxquery.rest.service.Status;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import static org.apache.vxquery.rest.Constants.HttpHeaderValues.CONTENT_TYPE_JSON;
import static org.apache.vxquery.rest.Constants.HttpHeaderValues.CONTENT_TYPE_XML;

/**
 * This class tests the success responses received for XQueries submitted. i.e we are submitting correct queries which
 * are expected to return a predictable result. All the parameters that are expected to be sent with query requests are
 * subjected to test in this test class
 *
 * @author Erandi Ganepola
 */
public class SuccessAsyncResponseTest extends AbstractRestServerTest {

    @Test
    public void testSimpleQuery001() throws Exception {
        QueryRequest request = new QueryRequest("1+1");
        request.setShowAbstractSyntaxTree(true);
        request.setShowOptimizedExpressionTree(true);
        request.setShowRuntimePlan(true);
        request.setShowTranslatedExpressionTree(true);
        request.setShowMetrics(false);

        runTest(CONTENT_TYPE_JSON, request);
        runTest(CONTENT_TYPE_XML, request);
    }

    @Test
    public void testSimpleQuery002() throws Exception {
        QueryRequest request = new QueryRequest("for $x in (1, 2.0, 3) return $x");
        request.setShowAbstractSyntaxTree(true);
        request.setShowOptimizedExpressionTree(true);
        request.setShowRuntimePlan(true);
        request.setShowTranslatedExpressionTree(true);
        request.setShowMetrics(true);

        runTest(CONTENT_TYPE_JSON, request);
        runTest(CONTENT_TYPE_XML, request);
    }

    @Test
    public void testSimpleQuery003() throws Exception {
        QueryRequest request = new QueryRequest("1+2+3");
        request.setShowAbstractSyntaxTree(false);
        request.setShowOptimizedExpressionTree(false);
        request.setShowRuntimePlan(false);
        request.setShowTranslatedExpressionTree(false);
        request.setShowMetrics(false);

        runTest(CONTENT_TYPE_JSON, request);
        runTest(CONTENT_TYPE_XML, request);
    }

    @Test
    public void testSimpleQuery004() throws Exception {
        QueryRequest request = new QueryRequest("fn:true()");
        request.setShowAbstractSyntaxTree(false);
        request.setShowOptimizedExpressionTree(false);
        request.setShowRuntimePlan(true);
        request.setShowTranslatedExpressionTree(false);
        request.setShowMetrics(false);

        runTest(CONTENT_TYPE_JSON, request);
        runTest(CONTENT_TYPE_XML, request);
    }

    private void runTest(String contentType, QueryRequest request) throws Exception {
        URI queryEndpointUri = RestUtils.buildQueryURI(request, restIpAddress, restPort);

        /*
         * ========== Query Response Testing ==========
         */
        // Testing the accuracy of VXQueryService class
        AsyncQueryResponse expectedAsyncQueryResponse = (AsyncQueryResponse) vxQueryService.execute(request);

        Assert.assertTrue(expectedAsyncQueryResponse.getResultUrl().startsWith(Constants.RESULT_URL_PREFIX));
        Assert.assertEquals(Status.SUCCESS.toString(), expectedAsyncQueryResponse.getStatus());
        Assert.assertNotEquals(0, expectedAsyncQueryResponse.getResultId());
        Assert.assertEquals(request.getStatement(), expectedAsyncQueryResponse.getStatement());

        if (request.isShowMetrics()) {
            Assert.assertTrue(expectedAsyncQueryResponse.getMetrics().getCompileTime() > 0);
        } else {
            Assert.assertTrue(expectedAsyncQueryResponse.getMetrics().getCompileTime() == 0);
        }

        if (request.isShowAbstractSyntaxTree()) {
            Assert.assertNotNull(expectedAsyncQueryResponse.getAbstractSyntaxTree());
        } else {
            Assert.assertNull(expectedAsyncQueryResponse.getAbstractSyntaxTree());
        }

        if (request.isShowTranslatedExpressionTree()) {
            Assert.assertNotNull(expectedAsyncQueryResponse.getTranslatedExpressionTree());
        } else {
            Assert.assertNull(expectedAsyncQueryResponse.getTranslatedExpressionTree());
        }

        if (request.isShowOptimizedExpressionTree()) {
            Assert.assertNotNull(expectedAsyncQueryResponse.getOptimizedExpressionTree());
        } else {
            Assert.assertNull(expectedAsyncQueryResponse.getOptimizedExpressionTree());
        }

        if (request.isShowRuntimePlan()) {
            Assert.assertNotNull(expectedAsyncQueryResponse.getRuntimePlan());
        } else {
            Assert.assertNull(expectedAsyncQueryResponse.getRuntimePlan());
        }

        checkMetrics(expectedAsyncQueryResponse, request.isShowMetrics());

        //Testing the accuracy of REST server and servlets
        AsyncQueryResponse actualAsyncQueryResponse = getQueryResponse(queryEndpointUri, contentType);
        Assert.assertNotNull(actualAsyncQueryResponse.getRequestId());
        Assert.assertTrue(actualAsyncQueryResponse.getResultUrl().startsWith(Constants.RESULT_URL_PREFIX));
        Assert.assertNotEquals(0, actualAsyncQueryResponse.getResultId());
        Assert.assertEquals(request.getStatement(), actualAsyncQueryResponse.getStatement());
        Assert.assertEquals(Status.SUCCESS.toString(), actualAsyncQueryResponse.getStatus());
        checkMetrics(actualAsyncQueryResponse, request.isShowMetrics());

        // Cannot check this because Runtime plan include some object IDs which differ
        // Assert.assertEquals(expectedAsyncQueryResponse.getRuntimePlan(), actualAsyncQueryResponse.getRuntimePlan());
        if (request.isShowRuntimePlan()) {
            Assert.assertNotNull(actualAsyncQueryResponse.getRuntimePlan());
        } else {
            Assert.assertNull(actualAsyncQueryResponse.getRuntimePlan());
        }

        Assert.assertEquals(normalize(expectedAsyncQueryResponse.getOptimizedExpressionTree()), normalize(actualAsyncQueryResponse.getOptimizedExpressionTree()));
        Assert.assertEquals(normalize(expectedAsyncQueryResponse.getTranslatedExpressionTree()), normalize(actualAsyncQueryResponse.getTranslatedExpressionTree()));
        Assert.assertEquals(normalize(expectedAsyncQueryResponse.getAbstractSyntaxTree()), normalize(actualAsyncQueryResponse.getAbstractSyntaxTree()));

        /*
         * ========== Query Result Response Testing ========
         */
        QueryResultRequest resultRequest = new QueryResultRequest(actualAsyncQueryResponse.getResultId());
        resultRequest.setShowMetrics(true);

        QueryResultResponse expectedResultResponse = (QueryResultResponse) vxQueryService.getResult(resultRequest);
        Assert.assertEquals(expectedResultResponse.getStatus(), Status.SUCCESS.toString());
        Assert.assertNotNull(expectedResultResponse.getResults());

        QueryResultResponse actualResultResponse = getQueryResultResponse(resultRequest, contentType);
        Assert.assertEquals(actualResultResponse.getStatus(), Status.SUCCESS.toString());
        Assert.assertNotNull(actualResultResponse.getResults());
        Assert.assertNotNull(actualResultResponse.getRequestId());
        Assert.assertEquals(normalize(expectedResultResponse.getResults()), normalize(actualResultResponse.getResults()));

        if (resultRequest.isShowMetrics()) {
            Assert.assertTrue(actualResultResponse.getMetrics().getElapsedTime() > 0);
        } else {
            Assert.assertTrue(actualResultResponse.getMetrics().getElapsedTime() == 0);
        }
    }

    /**
     * Submit a {@link QueryRequest} and fetth the resulting {@link AsyncQueryResponse}
     *
     * @param uri     uri of the GET request
     * @param accepts application/json | application/xml
     * @return Response received for the query request
     * @throws Exception
     */
    private static AsyncQueryResponse getQueryResponse(URI uri, String accepts) throws Exception {
        CloseableHttpClient httpClient = HttpClients.custom()
                                                 .setConnectionTimeToLive(20, TimeUnit.SECONDS)
                                                 .build();

        try {
            HttpGet request = new HttpGet(uri);
            request.setHeader(HttpHeaders.ACCEPT, accepts);

            try (CloseableHttpResponse httpResponse = httpClient.execute(request)) {
                Assert.assertEquals(HttpResponseStatus.OK.code(), httpResponse.getStatusLine().getStatusCode());
                Assert.assertEquals(accepts, httpResponse.getFirstHeader(HttpHeaders.CONTENT_TYPE).getValue());

                HttpEntity entity = httpResponse.getEntity();
                Assert.assertNotNull(entity);

                String response = RestUtils.readEntity(entity);
                return RestUtils.mapEntity(response, AsyncQueryResponse.class, accepts);
            }
        } finally {
            HttpClientUtils.closeQuietly(httpClient);
        }
    }

    /**
     * Fetch the {@link QueryResultResponse} from query result endpoint once the corresponding {@link
     * QueryResultRequest} is given.
     *
     * @param resultRequest {@link QueryResultRequest}
     * @param accepts       expected <pre>Accepts</pre> header in responses
     * @return query result reponse received
     * @throws Exception
     */
    private static QueryResultResponse getQueryResultResponse(QueryResultRequest resultRequest, String accepts) throws Exception {
        URI queryResultEndpointUri = RestUtils.buildQueryResultURI(resultRequest, restIpAddress, restPort);

        CloseableHttpClient httpClient = HttpClients.custom()
                                                 .setConnectionTimeToLive(20, TimeUnit.SECONDS)
                                                 .build();

        try {
            HttpGet request = new HttpGet(queryResultEndpointUri);
            request.setHeader(HttpHeaders.ACCEPT, accepts);

            try (CloseableHttpResponse httpResponse = httpClient.execute(request)) {
                Assert.assertEquals(accepts, httpResponse.getFirstHeader(HttpHeaders.CONTENT_TYPE).getValue());
                Assert.assertEquals(httpResponse.getStatusLine().getStatusCode(), HttpResponseStatus.OK.code());

                HttpEntity entity = httpResponse.getEntity();
                Assert.assertNotNull(entity);

                String response = RestUtils.readEntity(entity);
                return RestUtils.mapEntity(response, QueryResultResponse.class, accepts);
            }
        } finally {
            HttpClientUtils.closeQuietly(httpClient);
        }
    }
}