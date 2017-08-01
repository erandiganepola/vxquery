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
import org.apache.vxquery.core.VXQuery;
import org.apache.vxquery.core.VXQueryConfig;
import org.apache.vxquery.exceptions.VXQueryRuntimeException;
import org.apache.vxquery.rest.RestServer;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.apache.vxquery.core.Constants.Properties.AVAILABLE_PROCESSORS;
import static org.apache.vxquery.core.Constants.Properties.HDFS_CONFIG;
import static org.apache.vxquery.core.Constants.Properties.JOIN_HASH_SIZE;
import static org.apache.vxquery.core.Constants.Properties.MAXIMUM_DATA_SIZE;

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
        AppArgs appArgs = new AppArgs();
        if (args != null) {
            CmdLineParser parser = new CmdLineParser(appArgs);
            try {
                parser.parseArgument(args);
            } catch (Exception e) {
                parser.printUsage(System.err);
                throw new VXQueryRuntimeException("Unable to parse app arguments", e);
            }
        }

        VXQueryConfig config = loadConfiguration(ccAppCtx.getCCContext().getClusterControllerInfo(), appArgs.getVxqueryConfig());
        vxQuery = new VXQuery(config);
        restServer = new RestServer(vxQuery, appArgs.getRestPort());
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

    /**
     * Loads properties from <pre>-appConfig foo/bar.properties</pre> file if specified in the app arguments.
     *
     * @param clusterControllerInfo cluster controller information
     * @param propertiesFile        vxquery configuration properties file, given by <pre>-appConfig</pre> option in app
     *                              argument
     * @return A new {@link VXQueryConfig} instance with either default properties or properties loaded from the
     * properties file given.
     */
    private VXQueryConfig loadConfiguration(ClusterControllerInfo clusterControllerInfo, String propertiesFile) {
        VXQueryConfig vxqConfig = new VXQueryConfig();
        if (propertiesFile != null) {
            try (InputStream in = new FileInputStream(propertiesFile)) {
                System.getProperties().load(in);
            } catch (IOException e) {
                LOGGER.log(Level.SEVERE, String.format("Error occurred when loading properties file %s", propertiesFile), e);
            }
        }

        vxqConfig.setAvailableProcessors(Integer.getInteger(AVAILABLE_PROCESSORS, 1));
        vxqConfig.setJoinHashSize(Long.getLong(JOIN_HASH_SIZE, -1));
        vxqConfig.setHdfsConf(System.getProperty(HDFS_CONFIG));
        vxqConfig.setMaximumDataSize(Long.getLong(MAXIMUM_DATA_SIZE, -1));

        vxqConfig.setHyracksClientIp(clusterControllerInfo.getClientNetAddress());
        vxqConfig.setHyracksClientPort(clusterControllerInfo.getClientNetPort());

        return vxqConfig;
    }

    public VXQuery getVxQuery() {
        return vxQuery;
    }

    public RestServer getRestServer() {
        return restServer;
    }

    /**
     * Application Arguments bean class
     */
    private class AppArgs {
        @Option(name = "-restPort", usage = "The port on which REST server starts")
        private int restPort = 8080;

        @Option(name = "-appConfig", usage = "Properties file location which includes VXQuery Application additional configuration")
        private String vxqueryConfig = null;

        public String getVxqueryConfig() {
            return vxqueryConfig;
        }

        public void setVxqueryConfig(String vxqueryConfig) {
            this.vxqueryConfig = vxqueryConfig;
        }

        public int getRestPort() {
            return restPort;
        }

        public void setRestPort(int restPort) {
            this.restPort = restPort;
        }
    }
}