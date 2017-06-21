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

import org.apache.hyracks.http.server.HttpServer;
import org.apache.hyracks.http.server.WebManager;
import org.apache.vxquery.rest.core.VXQuery;
import org.apache.vxquery.rest.core.VXQueryConfig;
import org.apache.vxquery.rest.servlet.QueryAPIServlet;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.apache.vxquery.rest.Constants.URLs.QUERY_ENDPOINT;

/**
 * REST Server class responsible for starting a new server on a given port.
 *
 * @author Erandi Ganepola
 */
public class RestServer {

    public static final Logger LOGGER = Logger.getLogger(RestServer.class.getName());

    private WebManager webManager;
    private VXQuery vxQuery;

    public RestServer() {
        vxQuery = new VXQuery(loadConfiguration());

        webManager = new WebManager();
        HttpServer restServer = new HttpServer(webManager.getBosses(), webManager.getWorkers(), 8085);
        restServer.addServlet(new QueryAPIServlet(vxQuery, restServer.ctx(), QUERY_ENDPOINT));
        webManager.add(restServer);
    }

    public void start() {
        LOGGER.log(Level.INFO, "Starting VXQuery");
        try {
            vxQuery.start();
        } catch (RuntimeException e) {
            LOGGER.log(Level.SEVERE, "Error occurred when starting VXQuery", e);
            throw e;
        }

        LOGGER.log(Level.CONFIG, "Starting rest server");
        try {
            webManager.start();
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error occurred when starting rest server", e);
            throw new IllegalStateException("Unable to start REST server", e);
        }
        LOGGER.log(Level.INFO, "Rest server started");

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                RestServer.this.stop();
            }
        });
    }

    public void stop() {
        LOGGER.log(Level.CONFIG, "Stopping rest server");
        try {
            webManager.stop();
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error occurred when stopping rest server", e);
            throw new IllegalStateException("Unable to stop REST server", e);
        }
        LOGGER.log(Level.INFO, "Rest server stopped");

        LOGGER.log(Level.INFO, "Stopping VXQuery");
        try {
            vxQuery.stop();
        } catch (RuntimeException e) {
            LOGGER.log(Level.SEVERE, "Error occurred when stopping VXQuery", e);
            throw e;
        }
    }

    private VXQueryConfig loadConfiguration() {
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

        return vxQueryConfig;
    }
}
