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
import org.apache.vxquery.rest.core.VXQuery;
import org.apache.vxquery.rest.request.QueryResultRequest;
import org.apache.vxquery.rest.response.QueryResultResponse;

import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

/**
 * Servlet to handle query results requests.
 *
 * @author Erandi Ganepola
 */
public class QueryResultAPIServlet extends RestAPIServlet {

    private VXQuery vxQuery;

    public QueryResultAPIServlet(VXQuery vxQuery, ConcurrentMap<String, Object> ctx, String... paths) {
        super(ctx, paths);
        this.vxQuery = vxQuery;
    }

    @Override
    protected void doHandle(IServletRequest request, IServletResponse response) {
        LOGGER.log(Level.INFO, String.format("Received a result request with resultId : %s", request.getParameter("resultId")));

        QueryResultRequest resultRequest = getQueryResultRequest(request);
        QueryResultResponse queryResponse = new QueryResultResponse();
        try {
            // TODO: 6/21/17 Implement result fetching
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error occurred when trying to get results for resultId: " + resultRequest.getResultId(), e);
            throw new IllegalArgumentException("Unable to fetch result for id : " + resultRequest.getResultId(), e);
        }
    }

    private QueryResultRequest getQueryResultRequest(IServletRequest request) {
        String uri = request.getHttpRequest().uri();
        long resultId;
        try {
            resultId = Long.parseLong(uri.substring(uri.lastIndexOf("/") + 1));
        } catch (NumberFormatException e) {
            LOGGER.log(Level.SEVERE, "Result ID could not be retrieved from URL");
            throw new IllegalArgumentException("Result ID is required as a path param");
        }

        QueryResultRequest resultRequest = new QueryResultRequest();
        resultRequest.setResultId(resultId);
        return resultRequest;
    }
}
