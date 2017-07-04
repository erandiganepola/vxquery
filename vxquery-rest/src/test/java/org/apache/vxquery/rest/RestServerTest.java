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
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.HttpClientUtils;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.vxquery.rest.core.Status;
import org.apache.vxquery.rest.response.QueryResponse;
import org.apache.vxquery.rest.response.QueryResultResponse;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

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
    private static URI queryEndpointUri;

    @BeforeClass
    public static void setUp() throws Exception {
        System.setProperty(Constants.Properties.VXQUERY_PROPERTIES_FILE, "src/test/resources/vxquery.properties");
        application = new VXQueryApplication();
        application.start();

        restServer = application.getRestServer();

        queryEndpointUri = new URIBuilder()
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
    }

    @Test
    public void testJSONResponses() throws URISyntaxException, IOException, InterruptedException {
        String httpQueryResponse = getResponse(queryEndpointUri);
        Assert.assertNotNull(httpQueryResponse);
        Assert.assertTrue(httpQueryResponse.length() > 0);

        ObjectMapper jsonMapper = new ObjectMapper();
        QueryResponse queryResponse = jsonMapper.readValue(httpQueryResponse, QueryResponse.class);

        Assert.assertEquals(Status.SUCCESS.toString(), queryResponse.getStatus());
        Assert.assertNotNull(queryResponse.getResultId());
        Assert.assertNotNull(queryResponse.getRequestId());
        Assert.assertNotNull(queryResponse.getResultUrl());

        URI queryResultEndpointUri = new URIBuilder()
                .setScheme("http")
                .setHost("localhost")
                .setPort(restServer.getPort())
                .setPath(QUERY_RESULT_ENDPOINT + String.valueOf(queryResponse.getResultId()))
                .build();

        QueryResultResponse queryResultResponse;
        do {
            String httpQueryResultResponse = getResponse(queryResultEndpointUri);
            Assert.assertNotNull(httpQueryResultResponse);
            Assert.assertTrue(httpQueryResultResponse.length() > 0);

            queryResultResponse = jsonMapper.readValue(httpQueryResultResponse, QueryResultResponse.class);

            if (Status.INCOMPLETE.toString().equals(queryResultResponse.getStatus())) {
                Thread.sleep(5000);
                continue;
            }

            Assert.assertEquals(Status.SUCCESS.toString(), queryResultResponse.getStatus());
            Assert.assertNotNull(queryResultResponse.getResults());
            Assert.assertNotNull(queryResultResponse.getRequestId());
            Assert.assertEquals(RESULT, queryResultResponse.getResults());
            break;
        } while (!Status.FAILED.toString().equals(queryResultResponse.getStatus()));
    }

    private static String getResponse(URI uri) throws IOException {
        CloseableHttpClient httpClient = HttpClients.custom()
                .setConnectionTimeToLive(20, TimeUnit.SECONDS)
                .build();

        StringBuilder responseBody = new StringBuilder();
        try {
            HttpGet request = new HttpGet(uri);

            try (CloseableHttpResponse httpQueryResponse = httpClient.execute(request)) {
                Assert.assertEquals(httpQueryResponse.getStatusLine().getStatusCode(), HttpResponseStatus.OK.code());


                HttpEntity entity = httpQueryResponse.getEntity();
                Assert.assertNotNull(entity);

                try (InputStream in = entity.getContent()) {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                    String line;
                    while ((line = reader.readLine()) != null) {
                        responseBody.append(line);
                    }
                }
            }
        } finally {
            HttpClientUtils.closeQuietly(httpClient);
        }

        return responseBody.toString();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        application.stop();
    }
}
