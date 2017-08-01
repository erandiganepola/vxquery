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
import org.apache.vxquery.core.Constants;
import org.apache.vxquery.core.Status;
import org.apache.vxquery.rest.request.QueryRequest;
import org.apache.vxquery.rest.request.QueryResultRequest;
import org.apache.vxquery.rest.response.QueryResponse;
import org.apache.vxquery.rest.response.QueryResultResponse;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import static org.apache.vxquery.core.Constants.HttpHeaderValues.CONTENT_TYPE_JSON;
import static org.apache.vxquery.core.Constants.HttpHeaderValues.CONTENT_TYPE_XML;

/**
 * This class tests the success responses received for XQueries submitted. i.e we are submitting correct queries which
 * are expected to return a predictable result. All the parameters that are expected to be sent with query requests are
 * subjected to test in this test class
 *
 * @author Erandi Ganepola
 */
public class SuccessResponseTest extends AbstractRestServerTest {

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

    private void runTest(String contentType, QueryRequest request) throws Exception {
        URI queryEndpointUri = RestUtils.buildQueryURI(request, "localhost", restPort);

        /*
         * ========== Query Response Testing ==========
         */
        // Testing the accuracy of VXQuery class
        QueryResponse expectedQueryResponse = (QueryResponse) vxQuery.execute(request);

        Assert.assertTrue(expectedQueryResponse.getResultUrl().startsWith(Constants.RESULT_URL_PREFIX));
        Assert.assertEquals(Status.SUCCESS.toString(), expectedQueryResponse.getStatus());
        Assert.assertNotEquals(0, expectedQueryResponse.getResultId());
        Assert.assertEquals(request.getStatement(), expectedQueryResponse.getStatement());

        if (request.isShowMetrics()) {
            Assert.assertTrue(expectedQueryResponse.getMetrics().getCompileTime() > 0);
        } else {
            Assert.assertTrue(expectedQueryResponse.getMetrics().getCompileTime() == 0);
        }

        if (request.isShowAbstractSyntaxTree()) {
            Assert.assertNotNull(expectedQueryResponse.getAbstractSyntaxTree());
        } else {
            Assert.assertNull(expectedQueryResponse.getAbstractSyntaxTree());
        }

        if (request.isShowTranslatedExpressionTree()) {
            Assert.assertNotNull(expectedQueryResponse.getTranslatedExpressionTree());
        } else {
            Assert.assertNull(expectedQueryResponse.getTranslatedExpressionTree());
        }

        if (request.isShowOptimizedExpressionTree()) {
            Assert.assertNotNull(expectedQueryResponse.getOptimizedExpressionTree());
        } else {
            Assert.assertNull(expectedQueryResponse.getOptimizedExpressionTree());
        }

        if (request.isShowRuntimePlan()) {
            Assert.assertNotNull(expectedQueryResponse.getRuntimePlan());
        } else {
            Assert.assertNull(expectedQueryResponse.getRuntimePlan());
        }

        checkMetrics(expectedQueryResponse, request.isShowMetrics());

        //Testing the accuracy of REST server and servlets
        QueryResponse actualQueryResponse = getQueryResponse(queryEndpointUri, contentType);
        Assert.assertNotNull(actualQueryResponse.getRequestId());
        Assert.assertTrue(actualQueryResponse.getResultUrl().startsWith(Constants.RESULT_URL_PREFIX));
        Assert.assertNotEquals(0, actualQueryResponse.getResultId());
        Assert.assertEquals(request.getStatement(), actualQueryResponse.getStatement());
        Assert.assertEquals(Status.SUCCESS.toString(), actualQueryResponse.getStatus());
        checkMetrics(actualQueryResponse, request.isShowMetrics());

        // Cannot check this because Runtime plan include some object IDs which differ
        // Assert.assertEquals(expectedQueryResponse.getRuntimePlan(), actualQueryResponse.getRuntimePlan());
        Assert.assertNotNull(actualQueryResponse.getRuntimePlan());
        Assert.assertEquals(normalize(expectedQueryResponse.getOptimizedExpressionTree()), normalize(actualQueryResponse.getOptimizedExpressionTree()));
        Assert.assertEquals(normalize(expectedQueryResponse.getTranslatedExpressionTree()), normalize(actualQueryResponse.getTranslatedExpressionTree()));
        Assert.assertEquals(normalize(expectedQueryResponse.getAbstractSyntaxTree()), normalize(actualQueryResponse.getAbstractSyntaxTree()));

        /*
         * ========== Query Result Response Testing ========
         */
        QueryResultRequest resultRequest = new QueryResultRequest(actualQueryResponse.getResultId());
        resultRequest.setShowMetrics(true);

        QueryResultResponse expectedResultResponse = (QueryResultResponse) vxQuery.getResult(resultRequest);
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

    private void checkMetrics(QueryResponse response, boolean showMetrics) {
        if (showMetrics) {
            Assert.assertTrue(response.getMetrics().getCompileTime() > 0);
            Assert.assertTrue(response.getMetrics().getElapsedTime() > 0);
        } else {
            Assert.assertTrue(response.getMetrics().getCompileTime() == 0);
            Assert.assertTrue(response.getMetrics().getElapsedTime() == 0);
        }
    }

    private static String normalize(String string) {
        return string.replace("\r\n", "").replace("\n", "").replace("\r", "");
    }

    /**
     * Submit a {@link QueryRequest} and fetth the resulting {@link QueryResponse}
     *
     * @param uri     uri of the GET request
     * @param accepts application/json | application/xml
     * @return Response received for the query request
     * @throws Exception
     */
    private static QueryResponse getQueryResponse(URI uri, String accepts) throws Exception {
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
                return RestUtils.mapEntity(response, QueryResponse.class, accepts);
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
        URI queryResultEndpointUri = RestUtils.buildQueryResultURI(resultRequest, "localhost", restPort);

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
