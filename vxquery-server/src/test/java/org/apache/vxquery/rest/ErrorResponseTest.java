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

import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.HttpClientUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.vxquery.app.util.RestUtils;
import org.apache.vxquery.rest.request.QueryRequest;
import org.apache.vxquery.rest.response.ErrorResponse;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import static org.apache.vxquery.rest.Constants.HttpHeaderValues.CONTENT_TYPE_JSON;
import static org.apache.vxquery.rest.Constants.HttpHeaderValues.CONTENT_TYPE_XML;

/**
 * Tests error codes of the possible error responses that can be received for erroneous queries.
 *
 * @author Erandi Ganepola
 */
public class ErrorResponseTest extends AbstractRestServerTest {

    @Test
    public void testInvalidInput01() throws Exception {
        QueryRequest request = new QueryRequest(null, "   ");
        runTest(request, CONTENT_TYPE_JSON, 405);
        runTest(request, CONTENT_TYPE_XML, 405);
    }

    @Test
    public void testInvalidInput02() throws Exception {
        QueryRequest request = new QueryRequest(null, "");
        runTest(request, CONTENT_TYPE_JSON, 405);
        runTest(request, CONTENT_TYPE_XML, 405);
    }

    @Test
    public void testInvalidQuery01() throws Exception {
        QueryRequest request = new QueryRequest(null, "for $x in (1,2,3) return $y");
        runTest(request, CONTENT_TYPE_JSON, 400);
        runTest(request, CONTENT_TYPE_XML, 400);
    }

    @Test
    public void testInvalidQuery02() throws Exception {
        QueryRequest request = new QueryRequest(null, "for x in (1,2,3) return $x");
        runTest(request, CONTENT_TYPE_JSON, 400);
        runTest(request, CONTENT_TYPE_XML, 400);
    }

    @Test
    public void testInvalidQuery03() throws Exception {
        QueryRequest request = new QueryRequest(null, "insert nodes <book></book> into doc(\"abcd.xml\")/books");
        runTest(request, CONTENT_TYPE_JSON, 400);
        runTest(request, CONTENT_TYPE_XML, 400);
    }

    @Test
    public void testInvalidQuery04() throws Exception {
        QueryRequest request = new QueryRequest(null, "delete nodes /a/b//node()");
        runTest(request, CONTENT_TYPE_JSON, 400);
        runTest(request, CONTENT_TYPE_XML, 400);
    }

    private void runTest(QueryRequest request, String accepts, int expectedStatusCode) throws Exception {
        URI queryEndpointUri = RestUtils.buildQueryURI(request, "localhost", restPort);

        CloseableHttpClient httpClient = HttpClients.custom()
                                                 .setConnectionTimeToLive(20, TimeUnit.SECONDS)
                                                 .build();

        ErrorResponse errorResponse = null;
        try {
            HttpGet httpGet = new HttpGet(queryEndpointUri);
            httpGet.setHeader(HttpHeaders.ACCEPT, accepts);

            try (CloseableHttpResponse httpResponse = httpClient.execute(httpGet)) {
                Assert.assertEquals(expectedStatusCode, httpResponse.getStatusLine().getStatusCode());
                Assert.assertEquals(accepts, httpResponse.getFirstHeader(HttpHeaders.CONTENT_TYPE).getValue());

                HttpEntity entity = httpResponse.getEntity();
                Assert.assertNotNull(entity);

                String response = RestUtils.readEntity(entity);
                errorResponse = RestUtils.mapEntity(response, ErrorResponse.class, accepts);
            }
        } finally {
            HttpClientUtils.closeQuietly(httpClient);
        }

        Assert.assertNotNull(errorResponse);
        Assert.assertNotNull(errorResponse.getError().getMessage());
        Assert.assertEquals(errorResponse.getError().getCode(), expectedStatusCode);
    }
}
