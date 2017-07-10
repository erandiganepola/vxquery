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
import org.apache.commons.httpclient.HttpStatus;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
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
import org.apache.vxquery.rest.response.QueryResultErrorResponse;
import org.apache.vxquery.rest.response.QueryResultResponse;
import org.apache.vxquery.rest.response.QueryResultSuccessResponse;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static org.apache.vxquery.rest.Constants.HttpHeaderValues.CONTENT_TYPE_JSON;
import static org.apache.vxquery.rest.Constants.HttpHeaderValues.CONTENT_TYPE_XML;
import static org.apache.vxquery.rest.Constants.Parameters.*;
import static org.apache.vxquery.rest.Constants.URLs.QUERY_ENDPOINT;

public class RestServerTest {

    private static final Logger LOGGER = Logger.getLogger(RestServerTest.class.getName());

    private static final String QUERY_RESULT_ENDPOINT = "/vxquery/query/result/";
    private static final String QUERY = "for $x in doc(\"src/test/resources/dblp.xml\")/dblp/proceedings where $x/year=1990 return $x/title";
    private static final String RESULT = "<title>Advances in Database Technology - EDBT&apos;90.  International Conference on Extending Database Technology, Venice, Italy, March 26-30, 1990, Proceedings</title>\n" +
            "<title>Proceedings of the Sixth International Conference on Data Engineering, February 5-9, 1990, Los Angeles, California, USA</title>\n" +
            "<title>ICDT&apos;90, Third International Conference on Database Theory, Paris, France, December 12-14, 1990, Proceedings</title>\n" +
            "<title>Proceedings of the Ninth ACM SIGACT-SIGMOD-SIGART Symposium on Principles of Database Systems, April 2-4, 1990, Nashville, Tennessee</title>\n" +
            "<title>16th International Conference on Very Large Data Bases, August 13-16, 1990, Brisbane, Queensland, Australia, Proceedings.</title>\n" +
            "<title>Proceedings of the 1990 ACM SIGMOD International Conference on Management of Data, Atlantic City, NJ, May 23-25, 1990.</title>\n";

    private static VXQueryApplication application;
    private static RestServer restServer;

    @BeforeClass
    public static void setUp() throws Exception {
        System.setProperty(Constants.Properties.VXQUERY_PROPERTIES_FILE, "src/test/resources/vxquery.properties");
        application = new VXQueryApplication();
        application.start();

        restServer = application.getRestServer();
    }

    @Test
    public void testJSONResponses() throws Exception {
        runTest(CONTENT_TYPE_JSON);
    }

    @Test
    public void testXMLResponses() throws Exception {
        runTest(CONTENT_TYPE_XML);
    }

    private void runTest(String contentType) throws Exception {
        URI queryEndpointUri = new URIBuilder()
                .setScheme("http")
                .setHost("localhost")
                .setPort(restServer.getPort())
                .setPath(QUERY_ENDPOINT)
                .addParameter(STATEMENT, QUERY)
                .addParameter(SHOW_AST, "true")
                .addParameter(SHOW_TET, "true")
                .addParameter(SHOW_OET, "true")
                .addParameter(SHOW_RP, "true")
                .build();

        QueryResponse queryResponse = getQueryResponse(queryEndpointUri, contentType);
        Assert.assertNotNull(queryResponse);

        Assert.assertEquals(Status.SUCCESS.toString(), queryResponse.getStatus());
        Assert.assertNotNull(queryResponse.getResultId());
        Assert.assertNotNull(queryResponse.getRequestId());
        Assert.assertNotNull(queryResponse.getResultUrl());
        Assert.assertNotNull(queryResponse.getAbstractSyntaxTree());
        Assert.assertNotNull(queryResponse.getTranslatedExpressionTree());
        Assert.assertNotNull(queryResponse.getOptimizedExpressionTree());
        Assert.assertNotNull(queryResponse.getRuntimePlan());

        QueryResultResponse resultResponse;
        do {
            resultResponse = getQueryResultResponse(queryResponse.getResultId(), contentType);
            Assert.assertNotNull(resultResponse);
            Assert.assertNotNull(resultResponse.getStatus());

            if (!Status.SUCCESS.toString().equals(resultResponse.getStatus())) {
                Assert.assertTrue(resultResponse instanceof QueryResultErrorResponse);
                Assert.assertEquals(Status.INCOMPLETE.toString(), resultResponse.getStatus());
                Thread.sleep(5000);
                continue;
            }

            Assert.assertEquals(Status.SUCCESS.toString(), resultResponse.getStatus());
            Assert.assertTrue(resultResponse instanceof QueryResultSuccessResponse);

            QueryResultSuccessResponse successResponse = (QueryResultSuccessResponse) resultResponse;
            Assert.assertNotNull(successResponse.getResults());
            Assert.assertNotNull(resultResponse.getRequestId());
            Assert.assertEquals(RESULT.replace("\n", ""), successResponse.getResults().replace("\n", ""));
            break;
        } while (!Status.FAILED.toString().equals(resultResponse.getStatus()));
    }

    private static QueryResponse getQueryResponse(URI uri, String accepts) throws IOException, JAXBException {
        CloseableHttpClient httpClient = HttpClients.custom()
                .setConnectionTimeToLive(20, TimeUnit.SECONDS)
                .build();

        try {
            HttpGet request = new HttpGet(uri);
            request.setHeader(HttpHeaders.ACCEPT, accepts);

            try (CloseableHttpResponse httpQueryResponse = httpClient.execute(request)) {
                Assert.assertEquals(httpQueryResponse.getStatusLine().getStatusCode(), HttpResponseStatus.OK.code());
                Assert.assertEquals(accepts, httpQueryResponse.getFirstHeader(HttpHeaders.CONTENT_TYPE).getValue());

                HttpEntity entity = httpQueryResponse.getEntity();
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
                .setPort(restServer.getPort())
                .setPath(QUERY_RESULT_ENDPOINT + String.valueOf(resultId))
                .build();

        CloseableHttpClient httpClient = HttpClients.custom()
                .setConnectionTimeToLive(20, TimeUnit.SECONDS)
                .build();

        try {
            HttpGet request = new HttpGet(queryResultEndpointUri);
            request.setHeader(HttpHeaders.ACCEPT, accepts);

            try (CloseableHttpResponse httpQueryResponse = httpClient.execute(request)) {
                Assert.assertEquals(accepts, httpQueryResponse.getFirstHeader(HttpHeaders.CONTENT_TYPE).getValue());

                HttpEntity entity = httpQueryResponse.getEntity();
                Assert.assertNotNull(entity);

                String response = readEntity(entity);

                if (httpQueryResponse.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                    return mapEntity(response, QueryResultSuccessResponse.class, accepts);
                } else {
                    return mapEntity(response, QueryResultErrorResponse.class, accepts);
                }
            }
        } finally {
            HttpClientUtils.closeQuietly(httpClient);
        }
    }

    private static String readEntity(HttpEntity entity) throws IOException {
        StringBuilder responseBody = new StringBuilder();

        try (InputStream in = entity.getContent()) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String line;
            while ((line = reader.readLine()) != null) {
                responseBody.append(line);
            }
        }
        return responseBody.toString();
    }

    private static <T> T mapEntity(String entity, Class<T> type, String contentType) throws IOException, JAXBException {
        switch (contentType) {
            case CONTENT_TYPE_JSON:
                ObjectMapper jsonMapper = new ObjectMapper();
                return jsonMapper.readValue(entity, type);
            case CONTENT_TYPE_XML:
                JAXBContext jaxbContext = JAXBContext.newInstance(type);
                Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
                return type.cast(unmarshaller.unmarshal(new StringReader(entity)));
        }

        throw new IllegalArgumentException("Entity didn't match any content type");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        application.stop();
    }
}
