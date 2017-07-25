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
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.vxquery.rest.core.Status;
import org.apache.vxquery.rest.response.QueryResponse;
import org.apache.vxquery.rest.response.QueryResultResponse;
import org.junit.Assert;
import org.junit.Test;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;

import static org.apache.vxquery.rest.Constants.HttpHeaderValues.CONTENT_TYPE_JSON;
import static org.apache.vxquery.rest.Constants.HttpHeaderValues.CONTENT_TYPE_XML;
import static org.apache.vxquery.rest.Constants.Parameters.SHOW_AST;
import static org.apache.vxquery.rest.Constants.Parameters.SHOW_OET;
import static org.apache.vxquery.rest.Constants.Parameters.SHOW_RP;
import static org.apache.vxquery.rest.Constants.Parameters.SHOW_TET;
import static org.apache.vxquery.rest.Constants.Parameters.STATEMENT;
import static org.apache.vxquery.rest.Constants.URLs.QUERY_ENDPOINT;
import static org.apache.vxquery.rest.Constants.URLs.QUERY_RESULT_ENDPOINT;

/**
 * This class tests the success responses received for XQueries submitted. i.e we are submitting correct queries which
 * are expected to return a predictable result.
 *
 * @author Erandi Ganepola
 */
public class RestServerSuccessResponseTest extends AbstractRestServerTest {

    private static final String QUERY = "for $x in doc(\"src/test/resources/dblp.xml\")/dblp/proceedings where $x/year=1990 return $x/title";
    private static final String RESULT = "<title>Advances in Database Technology - EDBT&apos;90.  International Conference on Extending Database Technology, Venice, Italy, March 26-30, 1990, Proceedings</title>\n" +
                                                 "<title>Proceedings of the Sixth International Conference on Data Engineering, February 5-9, 1990, Los Angeles, California, USA</title>\n" +
                                                 "<title>ICDT&apos;90, Third International Conference on Database Theory, Paris, France, December 12-14, 1990, Proceedings</title>\n" +
                                                 "<title>Proceedings of the Ninth ACM SIGACT-SIGMOD-SIGART Symposium on Principles of Database Systems, April 2-4, 1990, Nashville, Tennessee</title>\n" +
                                                 "<title>16th International Conference on Very Large Data Bases, August 13-16, 1990, Brisbane, Queensland, Australia, Proceedings.</title>\n" +
                                                 "<title>Proceedings of the 1990 ACM SIGMOD International Conference on Management of Data, Atlantic City, NJ, May 23-25, 1990.</title>\n";

    // TODO: 7/17/17 Use xtests ghcnd dataset for tests

    @Test
    public void testSimpleQuery001() throws Exception {
        runTest(CONTENT_TYPE_JSON, "1+1", "2");
        runTest(CONTENT_TYPE_XML, "1+1", "2");
    }

    @Test
    public void testSimpleQuery002() throws Exception {
        runTest(CONTENT_TYPE_JSON, "fn:true()", "true");
        runTest(CONTENT_TYPE_XML, "fn:true()", "true");
    }

    @Test
    public void testSimpleQuery003() throws Exception {
        runTest(CONTENT_TYPE_JSON, "fn:false()", "false");
        runTest(CONTENT_TYPE_XML, "fn:false()", "false");
    }

    @Test
    public void testSimpleQuery004() throws Exception {
        runTest(CONTENT_TYPE_JSON, "for $x in (1, 2.0, 3) return $x", "123");
        runTest(CONTENT_TYPE_XML, "for $x in (1, 2.0, 3) return $x", "123");
    }

    @Test
    public void testSimpleQuery005() throws Exception {
        runTest(CONTENT_TYPE_JSON, "for $x in (1, 2, 3), $y in ('a', 'b', 'c') for $z in (1, 2) return ($x, $y, $z)",
                "1a11a21b11b21c11c22a12a22b12b22c12c23a13a23b13b23c13c2");
        runTest(CONTENT_TYPE_XML, "for $x in (1, 2, 3), $y in ('a', 'b', 'c') for $z in (1, 2) return ($x, $y, $z)",
                "1a11a21b11b21c11c22a12a22b12b22c12c23a13a23b13b23c13c2");
    }

    @Test
    public void testComplexQuery001() throws Exception {
        runTest(CONTENT_TYPE_JSON, QUERY, RESULT);
        runTest(CONTENT_TYPE_XML, QUERY, RESULT);
    }

    private void runTest(String contentType, String query, String result) throws Exception {
        URI queryEndpointUri = new URIBuilder()
                                       .setScheme("http")
                                       .setHost("localhost")
                                       .setPort(restPort)
                                       .setPath(QUERY_ENDPOINT)
                                       .addParameter(STATEMENT, query)
                                       .addParameter(SHOW_AST, "true")
                                       .addParameter(SHOW_TET, "true")
                                       .addParameter(SHOW_OET, "true")
                                       .addParameter(SHOW_RP, "true")
                                       .build();

        QueryResponse queryResponse = getQueryResponse(queryEndpointUri, contentType);
        Assert.assertNotNull(queryResponse);

        Assert.assertEquals(Status.SUCCESS.toString(), queryResponse.getStatus());
        Assert.assertNotEquals(0, queryResponse.getResultId());
        Assert.assertNotNull(queryResponse.getRequestId());
        Assert.assertNotNull(queryResponse.getResultUrl());
        Assert.assertNotNull(queryResponse.getAbstractSyntaxTree());
        Assert.assertNotNull(queryResponse.getTranslatedExpressionTree());
        Assert.assertNotNull(queryResponse.getOptimizedExpressionTree());
        Assert.assertNotNull(queryResponse.getRuntimePlan());
        Assert.assertEquals(query, queryResponse.getStatement());

        QueryResultResponse resultResponse = getQueryResultResponse(queryResponse.getResultId(), contentType);
        Assert.assertNotNull(resultResponse);
        Assert.assertNotNull(resultResponse.getStatus());
        Assert.assertEquals(resultResponse.getStatus(), Status.SUCCESS.toString());
        Assert.assertNotNull(resultResponse.getResults());
        Assert.assertNotNull(resultResponse.getRequestId());
        Assert.assertEquals(result.replace("\n", ""), resultResponse.getResults().replace("\n", ""));
    }

    private static QueryResponse getQueryResponse(URI uri, String accepts) throws IOException, JAXBException {
        CloseableHttpClient httpClient = HttpClients.custom()
                                                 .setConnectionTimeToLive(20, TimeUnit.SECONDS)
                                                 .build();

        try {
            HttpGet request = new HttpGet(uri);
            request.setHeader(HttpHeaders.ACCEPT, accepts);

            try (CloseableHttpResponse httpResponse = httpClient.execute(request)) {
                Assert.assertEquals(httpResponse.getStatusLine().getStatusCode(), HttpResponseStatus.OK.code());
                Assert.assertEquals(accepts, httpResponse.getFirstHeader(HttpHeaders.CONTENT_TYPE).getValue());

                HttpEntity entity = httpResponse.getEntity();
                Assert.assertNotNull(entity);

                String response = readEntity(entity);
                return mapEntity(response, QueryResponse.class, accepts);
            }
        } finally {
            HttpClientUtils.closeQuietly(httpClient);
        }
    }

    private static QueryResultResponse getQueryResultResponse(long resultId, String accepts) throws IOException, URISyntaxException, JAXBException {
        URI queryResultEndpointUri = new URIBuilder()
                                             .setScheme("http")
                                             .setHost("localhost")
                                             .setPort(restPort)
                                             .setPath(QUERY_RESULT_ENDPOINT.replace("*", String.valueOf(resultId)))
                                             .build();

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

                String response = readEntity(entity);
                return mapEntity(response, QueryResultResponse.class, accepts);
            }
        } finally {
            HttpClientUtils.closeQuietly(httpClient);
        }
    }
}
