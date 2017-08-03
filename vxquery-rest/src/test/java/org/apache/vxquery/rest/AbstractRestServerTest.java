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

import org.apache.vxquery.app.VXQueryApplication;
import org.apache.vxquery.app.util.LocalClusterUtil;
import org.apache.vxquery.rest.service.VXQueryConfig;
import org.apache.vxquery.rest.service.VXQueryService;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Abstract test class to be used for {@link VXQueryApplication} related tests. These tests are expected to use the REST
 * API for querying and fetching results
 *
 * @author Erandi Ganepola
 */
public class AbstractRestServerTest {

    protected static LocalClusterUtil vxqueryLocalCluster = new LocalClusterUtil();
    protected static String restIpAddress;
    protected static int restPort;
    protected static VXQueryService vxQueryService;

    @BeforeClass
    public static void setUp() throws Exception {
        vxqueryLocalCluster.init(new VXQueryConfig());
        vxQueryService = vxqueryLocalCluster.getVxQueryService();
        restIpAddress = vxqueryLocalCluster.getIpAddress();
        restPort = vxqueryLocalCluster.getRestPort();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        vxqueryLocalCluster.deinit();
    }
}
