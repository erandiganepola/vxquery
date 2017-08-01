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

import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.vxquery.app.VXQueryApplication;
import org.apache.vxquery.core.VXQuery;
import org.apache.vxquery.core.VXQueryConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.net.InetAddress;
import java.nio.file.Files;
import java.util.Arrays;

/**
 * Abstract test class to be used for {@link VXQueryApplication} related tests. These tests are expected to use the REST
 * API for querying and fetching results
 *
 * @author Erandi Ganepola
 */
public class AbstractRestServerTest {

    protected static final int restPort = 8085;

    private static ClusterControllerService clusterControllerService;
    private static NodeControllerService nodeControllerService;

    protected static VXQuery vxQuery;

    @BeforeClass
    public static void setUp() throws Exception {
        startLocalHyracks();

        CCConfig ccConfig = clusterControllerService.getCCConfig();
        VXQueryConfig config = new VXQueryConfig();
        config.setHyracksClientIp(ccConfig.clientNetIpAddress);
        config.setHyracksClientPort(ccConfig.clientNetPort);
        vxQuery = new VXQuery(config);
        vxQuery.start();
    }

    /**
     * Start local virtual cluster with cluster controller node and node controller nodes. IP address provided for node
     * controller is localhost. Unassigned ports 39000 and 39001 are used for client and cluster port respectively.
     *
     * @throws Exception
     */
    private static void startLocalHyracks() throws Exception {
        String localAddress = InetAddress.getLocalHost().getHostAddress();
        CCConfig ccConfig = new CCConfig();
        ccConfig.clientNetIpAddress = localAddress;
        ccConfig.clientNetPort = 39000;
        ccConfig.clusterNetIpAddress = localAddress;
        ccConfig.clusterNetPort = 39001;
        ccConfig.httpPort = 39002;
        ccConfig.profileDumpPeriod = 10000;
        ccConfig.appCCMainClass = VXQueryApplication.class.getName();
        ccConfig.appArgs = Arrays.asList("-restPort", String.valueOf(restPort));
        clusterControllerService = new ClusterControllerService(ccConfig);
        clusterControllerService.start();

        NCConfig ncConfig = new NCConfig();
        ncConfig.ccHost = "localhost";
        ncConfig.ccPort = 39001;
        ncConfig.clusterNetIPAddress = localAddress;
        ncConfig.dataIPAddress = localAddress;
        ncConfig.resultIPAddress = localAddress;
        ncConfig.nodeId = "nc";
        ncConfig.ioDevices = Files.createTempDirectory(ncConfig.nodeId).toString();
        nodeControllerService = new NodeControllerService(ncConfig);
        nodeControllerService.start();
    }


    /**
     * Shuts down the virtual cluster, along with all nodes and node execution, network and queue managers.
     *
     * @throws Exception
     */
    private static void stopLocalHyracks() throws Exception {
        nodeControllerService.stop();
        clusterControllerService.stop();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        vxQuery.stop();
        stopLocalHyracks();
    }
}
