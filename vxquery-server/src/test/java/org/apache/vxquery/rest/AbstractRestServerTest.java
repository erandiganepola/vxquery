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

import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.vxquery.app.VXQueryApplication;
import org.apache.vxquery.app.core.VXQuery;
import org.apache.vxquery.app.core.VXQueryConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.InetAddress;
import java.nio.file.Files;

import static org.apache.vxquery.rest.Constants.HttpHeaderValues.CONTENT_TYPE_JSON;
import static org.apache.vxquery.rest.Constants.HttpHeaderValues.CONTENT_TYPE_XML;
import static org.apache.vxquery.rest.Constants.Properties.REST_SERVER_PORT;

public class AbstractRestServerTest {

    private static ClusterControllerService clusterControllerService;
    private static NodeControllerService nodeControllerService;

    protected static int restPort;
    protected static VXQuery vxQuery;

    @BeforeClass
    public static void setUp() throws Exception {
        System.setProperty(Constants.Properties.VXQUERY_PROPERTIES_FILE, "src/test/resources/vxquery.properties");

        startLocalHyracks();

        CCConfig ccConfig = clusterControllerService.getCCConfig();
        VXQueryConfig config = new VXQueryConfig();
        config.setHyracksClientIp(ccConfig.clientNetIpAddress);
        config.setHyracksClientPort(ccConfig.clientNetPort);
        vxQuery = new VXQuery(config);
        vxQuery.start();

        restPort = Integer.parseInt(System.getProperty(REST_SERVER_PORT, "8085"));
    }

    /**
     * Start local virtual cluster with cluster controller node and node controller nodes. IP address provided for node
     * controller is localhost. Unassigned ports 39000 and 39001 are used for client and cluster port respectively.
     * Creates a new Hyracks connection with the IP address and client ports.
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

    public static String readEntity(HttpEntity entity) throws IOException {
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

    public static <T> T mapEntity(String entity, Class<T> type, String contentType) throws IOException, JAXBException {
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
        vxQuery.stop();
        stopLocalHyracks();
    }
}
