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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.vxquery.rest.core.VXQuery;
import org.apache.vxquery.rest.request.QueryRequest;
import org.apache.vxquery.rest.response.QueryResponse;

import java.io.IOException;
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
    protected void doHandle(IServletRequest request, IServletResponse response) throws IOException {
        LOGGER.log(Level.INFO, String.format("Received a query request with query : %s", request.getParameter("statement")));

        QueryRequest queryRequest = getQueryRequest(request);
        QueryResponse queryResponse = null;
        try {
            queryResponse = vxQuery.execute(queryRequest);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error occurred when trying to execute query : " + queryRequest.getStatement(), e);
            throw new IllegalArgumentException("Unable to execute the query given", e);
        }

        ObjectMapper jsonMapper = new ObjectMapper();
        String jsonString = jsonMapper.writeValueAsString(queryResponse);
        LOGGER.info(String.format("Query response : %s", jsonString));
        response.writer().print(jsonString);
    }

    private QueryRequest getQueryRequest(IServletRequest request) {
        if (request.getParameter(STATEMENT) == null) {
            throw new IllegalArgumentException("Parameter 'statement' is required to handle the request");
        }

        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setStatement(request.getParameter(STATEMENT));
        queryRequest.setCompileOnly("true".equals(request.getParameter(COMPILE_ONLY)));
        queryRequest.setShowMetrics("true".equals(request.getParameter(METRICS)));

        queryRequest.setShowAbstractSyntaxTree("true".equals(request.getParameter(SHOW_AST)));
        queryRequest.setShowTranslatedExpressionTree("true".equals(request.getParameter(SHOW_TET)));
        queryRequest.setShowOptimizedExpressionTree("true".equals(request.getParameter(SHOW_OET)));
        queryRequest.setShowRuntimePlan("true".equals(request.getParameter(SHOW_RP)));

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
