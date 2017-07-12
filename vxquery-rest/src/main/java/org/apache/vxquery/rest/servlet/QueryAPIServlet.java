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
import org.apache.vxquery.rest.core.VXQuery;
import org.apache.vxquery.rest.exceptions.VXQueryServletRuntimeException;
import org.apache.vxquery.rest.request.QueryRequest;
import org.apache.vxquery.rest.response.APIResponse;
import org.apache.vxquery.rest.response.Error;

import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

import static org.apache.vxquery.rest.Constants.Parameters.*;

/**
 * Servlet to handle query requests.
 *
 * @author Erandi Ganepola
 */
public class QueryAPIServlet extends RestAPIServlet {

    private VXQuery vxQuery;

    public QueryAPIServlet(VXQuery vxQuery, ConcurrentMap<String, Object> ctx, String... paths) {
        super(ctx, paths);
        this.vxQuery = vxQuery;
    }

    @Override
    protected APIResponse doHandle(IServletRequest request) {
        LOGGER.log(Level.INFO, String.format("Received a query request with query : %s", request.getParameter("statement")));

        QueryRequest queryRequest;
        try {
            queryRequest = getQueryRequest(request);
        } catch (IllegalArgumentException e) {
            return APIResponse.newErrorResponse(null, Error.builder()
                                                              .withCode(405)
                                                              .withMessage("Invalid input")
                                                              .build());
        }

        try {
            return vxQuery.execute(queryRequest);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error occurred when trying to execute query : " + queryRequest.getStatement(), e);
            throw new VXQueryServletRuntimeException("Unable to execute the query given", e);
        }
    }

    private QueryRequest getQueryRequest(IServletRequest request) {
        if (request.getParameter(STATEMENT) == null) {
            throw new IllegalArgumentException("Parameter 'statement' is required to handle the request");
        }

        QueryRequest queryRequest = new QueryRequest(UUID.randomUUID().toString(), request.getParameter(STATEMENT));
        queryRequest.setCompileOnly(Boolean.parseBoolean(request.getParameter(COMPILE_ONLY)));
        queryRequest.setShowMetrics(Boolean.parseBoolean(request.getParameter(METRICS)));

        queryRequest.setShowAbstractSyntaxTree(Boolean.parseBoolean(request.getParameter(SHOW_AST)));
        queryRequest.setShowTranslatedExpressionTree(Boolean.parseBoolean(request.getParameter(SHOW_TET)));
        queryRequest.setShowOptimizedExpressionTree(Boolean.parseBoolean(request.getParameter(SHOW_OET)));
        queryRequest.setShowRuntimePlan(Boolean.parseBoolean(request.getParameter(SHOW_RP)));

        if (request.getParameter(OPTIMIZATION) != null) {
            queryRequest.setOptimization(Integer.parseInt(request.getParameter(OPTIMIZATION)));
        }
        if (request.getParameter(FRAME_SIZE) != null) {
            queryRequest.setFrameSize(Integer.parseInt(request.getParameter(FRAME_SIZE)));
        }
        if (request.getParameter(REPEAT_EXECUTIONS) != null) {
            queryRequest.setRepeatExecutions(Integer.parseInt(request.getParameter(REPEAT_EXECUTIONS)));
        }

        return queryRequest;
    }
}
