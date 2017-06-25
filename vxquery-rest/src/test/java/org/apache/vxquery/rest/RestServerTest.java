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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.vxquery.rest.core.Status;
import org.apache.vxquery.rest.response.QueryResponse;
import org.apache.vxquery.rest.response.QueryResultResponse;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.logging.Logger;

import static org.apache.http.client.utils.HttpClientUtils.closeQuietly;
import static org.apache.vxquery.rest.Constants.Parameters.*;
import static org.apache.vxquery.rest.Constants.URLs.QUERY_ENDPOINT;

public class RestServerTest {

    private static final Logger LOGGER = Logger.getLogger(RestServerTest.class.getName());

    private static final String QUERY_RESULT_ENDPOINT = "/vxquery/query/result/";
    private static final String QUERY = "for $x in doc(\"dblp.xml\")/dblp/proceedings where $x/year=1990 return $x/title";
    private static final String RESULT = "<title>Advances in Database Technology - EDBT&apos;90.  International Conference on Extending Database Technology, Venice, Italy, March 26-30, 1990, Proceedings</title>\n" +
            "<title>Proceedings of the Sixth International Conference on Data Engineering, February 5-9, 1990, Los Angeles, California, USA</title>\n" +
            "<title>ICDT&apos;90, Third International Conference on Database Theory, Paris, France, December 12-14, 1990, Proceedings</title>\n" +
            "<title>Proceedings of the Ninth ACM SIGACT-SIGMOD-SIGART Symposium on Principles of Database Systems, April 2-4, 1990, Nashville, Tennessee</title>\n" +
            "<title>16th International Conference on Very Large Data Bases, August 13-16, 1990, Brisbane, Queensland, Australia, Proceedings.</title>\n" +
            "<title>Proceedings of the 1990 ACM SIGMOD International Conference on Management of Data, Atlantic City, NJ, May 23-25, 1990.</title>\n";

    private static final RestServer restServer = new RestServer();

    @BeforeClass
    public static void setUp() throws Exception {
        restServer.start();
    }

    @Test
    public void testRESTServer() throws InterruptedException, URISyntaxException, IOException {
        HttpClient httpClient = HttpClientBuilder.create().build();

        URI queryEndpointUri = new URIBuilder()
                .setScheme("http")
                .setHost("localhost")
                .setPort(8085)
                .setPath(QUERY_ENDPOINT)
                .addParameter(STATEMENT, QUERY)
                .addParameter(SHOW_AST, "true")
                .addParameter(SHOW_TET, "true")
                .addParameter(SHOW_OET, "true")
                .addParameter(SHOW_RP, "true")
                .build();

        HttpGet queryRequest = new HttpGet(queryEndpointUri);
        HttpResponse queryHttpResponse = httpClient.execute(queryRequest);
        Assert.assertEquals(queryHttpResponse.getStatusLine().getStatusCode(), HttpResponseStatus.OK.code());

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(queryHttpResponse.getEntity().getContent()));
        StringBuilder result = new StringBuilder();
        String line = "";
        while ((line = bufferedReader.readLine()) != null) {
            result.append(line);
        }
        bufferedReader.close();
        closeQuietly(queryHttpResponse);

        ObjectMapper jsonMapper = new ObjectMapper();
        QueryResponse queryResponse = jsonMapper.readValue(result.toString(), QueryResponse.class);

        Assert.assertEquals(Status.SUCCESS.toString(), queryResponse.getStatus());
        Assert.assertNotNull(queryResponse.getResultId());
        Assert.assertNotNull(queryResponse.getRequestId());
        Assert.assertNotNull(queryResponse.getResultUrl());

        // TODO: 6/25/17 Modify this not to use time bound waits for query completion
        Thread.sleep(20000);

        URI queryResultEndpointUri = new URIBuilder()
                .setScheme("http")
                .setHost("localhost")
                .setPort(8085)
                .setPath(QUERY_RESULT_ENDPOINT + String.valueOf(queryResponse.getResultId()))
                .build();

        HttpGet queryResultRequest = new HttpGet(queryResultEndpointUri);
        HttpResponse queryResultHttpResponse = httpClient.execute(queryResultRequest);
        Assert.assertEquals(queryResultHttpResponse.getStatusLine().getStatusCode(), HttpResponseStatus.OK.code());

        bufferedReader = new BufferedReader(new InputStreamReader(queryResultHttpResponse.getEntity().getContent()));
        result = new StringBuilder();
        line = "";
        while ((line = bufferedReader.readLine()) != null) {
            result.append(line);
        }
        bufferedReader.close();

        jsonMapper = new ObjectMapper();
        QueryResultResponse queryResultResponse = jsonMapper.readValue(result.toString(), QueryResultResponse.class);

        Assert.assertEquals(Status.SUCCESS.toString(), queryResultResponse.getStatus());
        Assert.assertNotNull(queryResultResponse.getResults());
        Assert.assertNotNull(queryResultResponse.getRequestId());
        closeQuietly(queryHttpResponse);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        restServer.stop();
    }
}
