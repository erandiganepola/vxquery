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

package org.apache.vxquery.rest.servlet;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.AbstractServlet;
import org.apache.hyracks.http.server.utils.HttpUtil;

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Abstract servlet to handle REST API requests.
 *
 * @author Erandi Ganepola
 */
public abstract class RestAPIServlet extends AbstractServlet {

    protected final Logger LOGGER;

    public RestAPIServlet(ConcurrentMap<String, Object> ctx, String... paths) {
        super(ctx, paths);
        LOGGER = Logger.getLogger(this.getClass().getName());
    }

    protected final void get(IServletRequest request, IServletResponse response) {
        try {
            initResponse(request, response);
            doHandle(request, response);
        } catch (IOException e) {
            response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
            LOGGER.log(Level.WARNING, "Failure handling request", e);
        }
    }

    private void initResponse(IServletRequest request, IServletResponse response) throws IOException {
        response.setStatus(HttpResponseStatus.OK);
        HttpUtil.setContentType(response, "application/json");
    }

    protected abstract void doHandle(IServletRequest request, IServletResponse response);
}
