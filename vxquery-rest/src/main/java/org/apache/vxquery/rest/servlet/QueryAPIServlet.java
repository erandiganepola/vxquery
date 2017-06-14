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

import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.vxquery.rest.VXQuery;
import org.apache.vxquery.rest.request.QueryRequest;
import org.apache.vxquery.rest.response.QueryResponse;

import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

/**
 * Servlet to handle query requests.
 *
 * @author Erandi Ganepola
 */
public class QueryAPIServlet extends RestAPIServlet {

    public QueryAPIServlet(ConcurrentMap<String, Object> ctx, String... paths) {
        super(ctx, paths);
    }

    @Override
    protected void doHandle(IServletRequest request, IServletResponse response) {
        LOGGER.log(Level.INFO, String.format("Received a query request with query : %s", request.getParameter("statement")));

        QueryRequest queryRequest = getQueryRequest(request);
        QueryResponse queryResponse = new QueryResponse();
        VXQuery vxQuery = new VXQuery(queryRequest, queryResponse);
        try {
            vxQuery.execute();
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error occurred when trying to execute query : " + queryRequest.getStatement(), e);
            throw new IllegalArgumentException("Unable to execute the query given", e);
        }
    }

    private QueryRequest getQueryRequest(IServletRequest request) {
        if (request.getParameter("statement") == null) {
            throw new IllegalArgumentException("Parameter 'statement' is required to handle the request");
        }

        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setStatement(request.getParameter("statement"));

        return queryRequest;
    }
}
