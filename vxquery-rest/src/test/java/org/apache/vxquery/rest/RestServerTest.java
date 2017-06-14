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
import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.Response;
import java.util.logging.Logger;

public class RestServerTest {

    private static final Logger LOGGER = Logger.getLogger(RestServerTest.class.getName());

    private static final RestServer restServer = new RestServer();

    @BeforeClass
    public static void setUp() throws Exception {
        restServer.start();
    }

    @Test
    public void testRESTServer() throws InterruptedException {
        MultivaluedHashMap<String, Object> queryParams = new MultivaluedHashMap<>();
        queryParams.add("statement", "for $x in doc(\"dblp.xml\")/dblp/proceedings return $x/title");

        Response response = new ResteasyClientBuilder()
                .build()
                .target("http://localhost:8085/query")
                .queryParams(queryParams)
                .request()
                .get();

        LOGGER.info("Status : " + response.getStatus());
        Assert.assertEquals(HttpResponseStatus.OK.code(), response.getStatus());
    }

    @AfterClass
    public static void tearDown() throws Exception {
        restServer.stop();
    }
}
