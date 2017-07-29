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
import org.apache.vxquery.rest.exceptions.VXQueryRuntimeException;
import org.apache.vxquery.rest.servlet.QueryAPIServlet;
import org.apache.vxquery.rest.servlet.QueryResultAPIServlet;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * REST Server class responsible for starting a new server on a given port.
 *
 * @author Erandi Ganepola
 */
public class RestServer {

    public static final Logger LOGGER = Logger.getLogger(RestServer.class.getName());

    private WebManager webManager;
    private int port;

    public RestServer(VXQuery vxQuery) {
        try {
            webManager = new WebManager();
            port = Integer.parseInt(System.getProperty(Constants.Properties.REST_SERVER_PORT, "8085"));
            HttpServer restServer = new HttpServer(webManager.getBosses(), webManager.getWorkers(), port);

            restServer.addServlet(new QueryAPIServlet(vxQuery, restServer.ctx(), Constants.URLs.QUERY_ENDPOINT));
            restServer.addServlet(new QueryResultAPIServlet(vxQuery, restServer.ctx(), Constants.URLs.QUERY_RESULT_ENDPOINT));
            webManager.add(restServer);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error occurred when creating rest server", e);
            throw e;
        }
    }

    public void start() {
        try {
            LOGGER.log(Level.FINE, "Starting rest server");
            webManager.start();
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error occurred when starting rest server", e);
            throw new VXQueryRuntimeException("Unable to start REST server", e);
        }
        LOGGER.log(Level.INFO, "Rest server started");
    }

    public void stop() {
        try {
            LOGGER.log(Level.FINE, "Stopping rest server");
            webManager.stop();
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error occurred when stopping VXQuery", e);
            throw new VXQueryRuntimeException("Error occurred when stopping rest server", e);
        }
        LOGGER.log(Level.INFO, "Rest server stopped");
    }

    public int getPort() {
        return port;
    }
}
