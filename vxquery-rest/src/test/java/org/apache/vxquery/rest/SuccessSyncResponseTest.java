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
import org.apache.vxquery.rest.response.AsyncQueryResponse;
import org.apache.vxquery.rest.response.SyncQueryResponse;
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
public class SuccessSyncResponseTest extends AbstractRestServerTest {

    @Test
    public void testSimpleQuery001() throws Exception {
        QueryRequest request = new QueryRequest("1+1");
        request.setShowAbstractSyntaxTree(true);
        request.setShowOptimizedExpressionTree(true);
        request.setShowRuntimePlan(true);
        request.setShowTranslatedExpressionTree(true);
        request.setShowMetrics(false);
        request.setAsync(false);

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
        request.setAsync(false);

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
        request.setAsync(false);

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
        request.setAsync(false);

        runTest(CONTENT_TYPE_JSON, request);
        runTest(CONTENT_TYPE_XML, request);
    }

    private void runTest(String contentType, QueryRequest request) throws Exception {
        URI queryEndpointUri = RestUtils.buildQueryURI(request, restIpAddress, restPort);

        /*
         * ========== Query Response Testing ==========
         */
        // Testing the accuracy of VXQueryService class
        SyncQueryResponse expectedSyncQueryResponse = (SyncQueryResponse) vxQueryService.execute(request);

        Assert.assertEquals(Status.SUCCESS.toString(), expectedSyncQueryResponse.getStatus());
        Assert.assertEquals(request.getStatement(), expectedSyncQueryResponse.getStatement());

        if (request.isShowMetrics()) {
            Assert.assertTrue(expectedSyncQueryResponse.getMetrics().getCompileTime() > 0);
        } else {
            Assert.assertTrue(expectedSyncQueryResponse.getMetrics().getCompileTime() == 0);
        }

        if (request.isShowAbstractSyntaxTree()) {
            Assert.assertNotNull(expectedSyncQueryResponse.getAbstractSyntaxTree());
        } else {
            Assert.assertNull(expectedSyncQueryResponse.getAbstractSyntaxTree());
        }

        if (request.isShowTranslatedExpressionTree()) {
            Assert.assertNotNull(expectedSyncQueryResponse.getTranslatedExpressionTree());
        } else {
            Assert.assertNull(expectedSyncQueryResponse.getTranslatedExpressionTree());
        }

        if (request.isShowOptimizedExpressionTree()) {
            Assert.assertNotNull(expectedSyncQueryResponse.getOptimizedExpressionTree());
        } else {
            Assert.assertNull(expectedSyncQueryResponse.getOptimizedExpressionTree());
        }

        if (request.isShowRuntimePlan()) {
            Assert.assertNotNull(expectedSyncQueryResponse.getRuntimePlan());
        } else {
            Assert.assertNull(expectedSyncQueryResponse.getRuntimePlan());
        }

        checkMetrics(expectedSyncQueryResponse, request.isShowMetrics());

        //Testing the accuracy of REST server and servlets
        SyncQueryResponse actualSyncQueryResponse = getQueryResponse(queryEndpointUri, contentType);
        Assert.assertNotNull(actualSyncQueryResponse.getRequestId());
        Assert.assertEquals(request.getStatement(), actualSyncQueryResponse.getStatement());
        Assert.assertEquals(Status.SUCCESS.toString(), actualSyncQueryResponse.getStatus());
        checkMetrics(actualSyncQueryResponse, request.isShowMetrics());

        // Cannot check this because Runtime plan include some object IDs which differ
        // Assert.assertEquals(expectedSyncQueryResponse.getRuntimePlan(), actualSyncQueryResponse.getRuntimePlan());
        if (request.isShowRuntimePlan()) {
            Assert.assertNotNull(actualSyncQueryResponse.getRuntimePlan());
        } else {
            Assert.assertNull(actualSyncQueryResponse.getRuntimePlan());
        }

        Assert.assertEquals(normalize(expectedSyncQueryResponse.getOptimizedExpressionTree()), normalize(actualSyncQueryResponse.getOptimizedExpressionTree()));
        Assert.assertEquals(normalize(expectedSyncQueryResponse.getTranslatedExpressionTree()), normalize(actualSyncQueryResponse.getTranslatedExpressionTree()));
        Assert.assertEquals(normalize(expectedSyncQueryResponse.getAbstractSyntaxTree()), normalize(actualSyncQueryResponse.getAbstractSyntaxTree()));

        /*
         * ========== Query Result Response Testing ========
         */
        String expectedResults = expectedSyncQueryResponse.getResults();
        Assert.assertNotNull(expectedResults);

        String actualResults = actualSyncQueryResponse.getResults();
        Assert.assertNotNull(actualResults);
        Assert.assertEquals(normalize(expectedResults), normalize(actualResults));
    }

    /**
     * Submit a {@link QueryRequest} and fetth the resulting {@link AsyncQueryResponse}
     *
     * @param uri     uri of the GET request
     * @param accepts application/json | application/xml
     * @return Response received for the query request
     * @throws Exception
     */
    private static SyncQueryResponse getQueryResponse(URI uri, String accepts) throws Exception {
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
                return RestUtils.mapEntity(response, SyncQueryResponse.class, accepts);
            }
        } finally {
            HttpClientUtils.closeQuietly(httpClient);
        }
    }
}