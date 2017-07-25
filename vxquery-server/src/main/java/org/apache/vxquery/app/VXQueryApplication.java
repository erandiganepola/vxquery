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

package org.apache.vxquery.app;

import org.apache.hyracks.api.application.ICCApplicationContext;
import org.apache.hyracks.api.application.ICCApplicationEntryPoint;
import org.apache.hyracks.api.client.ClusterControllerInfo;
import org.apache.vxquery.rest.Constants;
import org.apache.vxquery.rest.RestServer;
import org.apache.vxquery.app.core.VXQuery;
import org.apache.vxquery.app.core.VXQueryConfig;
import org.apache.vxquery.rest.exceptions.VXQueryRuntimeException;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Main class responsible for starting the {@link RestServer} and {@link VXQuery} classes.
 *
 * @author Erandi Ganepola
 */
public class VXQueryApplication implements ICCApplicationEntryPoint {

    private static final Logger LOGGER = Logger.getLogger(VXQueryApplication.class.getName());

    private VXQuery vxQuery;
    private RestServer restServer;

    @Override
    public void start(ICCApplicationContext ccAppCtx, String[] args) throws Exception {
        VXQueryConfig config = loadConfiguration(ccAppCtx.getCCContext().getClusterControllerInfo());
        vxQuery = new VXQuery(config);
        restServer = new RestServer(vxQuery);
    }

    public synchronized void stop() {
        try {
            LOGGER.log(Level.INFO, "Stopping REST server");
            restServer.stop();

            LOGGER.log(Level.INFO, "Stopping VXQuery");
            vxQuery.stop();
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error occurred when stopping the application", e);
        }
    }

    @Override
    public void startupCompleted() throws Exception {
        try {
            LOGGER.log(Level.INFO, "Starting VXQuery");
            vxQuery.start();
            LOGGER.log(Level.INFO, "VXQuery started successfully");

            LOGGER.log(Level.INFO, "Starting REST server");
            restServer.start();
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error occurred when starting application", e);
            stop();
            throw new VXQueryRuntimeException("Error occurred when starting application", e);
        }
    }

    private VXQueryConfig loadConfiguration(ClusterControllerInfo clusterControllerInfo) {
        VXQueryConfig vxQueryConfig = new VXQueryConfig();
        String file = System.getProperty(Constants.Properties.VXQUERY_PROPERTIES_FILE);
        if (file != null) {
            try (InputStream in = new FileInputStream(file)) {
                System.getProperties().load(in);
            } catch (IOException e) {
                LOGGER.log(Level.SEVERE, String.format("Error occurred when loading properties file %s", file), e);
            }
        }

        // TODO: 6/21/17 Load more properties
        vxQueryConfig.setHyracksClientIp(clusterControllerInfo.getClientNetAddress());
        vxQueryConfig.setHyracksClientPort(clusterControllerInfo.getClientNetPort());

        return vxQueryConfig;
    }

    public VXQuery getVxQuery() {
        return vxQuery;
    }

    public RestServer getRestServer() {
        return restServer;
    }
}
