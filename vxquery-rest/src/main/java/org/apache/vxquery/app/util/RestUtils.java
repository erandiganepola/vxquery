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

package org.apache.vxquery.app.util;

import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.client.utils.URIBuilder;
import org.apache.vxquery.rest.request.QueryRequest;
import org.apache.vxquery.rest.request.QueryResultRequest;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;

import static org.apache.vxquery.rest.Constants.HttpHeaderValues.CONTENT_TYPE_JSON;
import static org.apache.vxquery.rest.Constants.HttpHeaderValues.CONTENT_TYPE_XML;
import static org.apache.vxquery.rest.Constants.MODE_ASYNC;
import static org.apache.vxquery.rest.Constants.MODE_SYNC;
import static org.apache.vxquery.rest.Constants.Parameters.COMPILE_ONLY;
import static org.apache.vxquery.rest.Constants.Parameters.FRAME_SIZE;
import static org.apache.vxquery.rest.Constants.Parameters.METRICS;
import static org.apache.vxquery.rest.Constants.Parameters.MODE;
import static org.apache.vxquery.rest.Constants.Parameters.OPTIMIZATION;
import static org.apache.vxquery.rest.Constants.Parameters.REPEAT_EXECUTIONS;
import static org.apache.vxquery.rest.Constants.Parameters.SHOW_AST;
import static org.apache.vxquery.rest.Constants.Parameters.SHOW_OET;
import static org.apache.vxquery.rest.Constants.Parameters.SHOW_RP;
import static org.apache.vxquery.rest.Constants.Parameters.SHOW_TET;
import static org.apache.vxquery.rest.Constants.Parameters.STATEMENT;
import static org.apache.vxquery.rest.Constants.URLs.QUERY_ENDPOINT;
import static org.apache.vxquery.rest.Constants.URLs.QUERY_RESULT_ENDPOINT;

public class RestUtils {

    private RestUtils() {
    }

    public static URI buildQueryURI(QueryRequest request, String restIpAddress, int restPort) throws URISyntaxException {
        return new URIBuilder()
                       .setScheme("http")
                       .setHost(restIpAddress)
                       .setPort(restPort)
                       .setPath(QUERY_ENDPOINT)
                       .addParameter(STATEMENT, request.getStatement())
                       .addParameter(COMPILE_ONLY, String.valueOf(request.isCompileOnly()))
                       .addParameter(OPTIMIZATION, String.valueOf(request.getOptimization()))
                       .addParameter(FRAME_SIZE, String.valueOf(request.getFrameSize()))
                       .addParameter(REPEAT_EXECUTIONS, String.valueOf(request.getRepeatExecutions()))
                       .addParameter(METRICS, String.valueOf(request.isShowMetrics()))
                       .addParameter(SHOW_AST, String.valueOf(request.isShowAbstractSyntaxTree()))
                       .addParameter(SHOW_TET, String.valueOf(request.isShowTranslatedExpressionTree()))
                       .addParameter(SHOW_OET, String.valueOf(request.isShowOptimizedExpressionTree()))
                       .addParameter(SHOW_RP, String.valueOf(request.isShowRuntimePlan()))
                       .addParameter(MODE, request.isAsync() ? MODE_ASYNC : MODE_SYNC)
                       .build();
    }

    public static URI buildQueryResultURI(QueryResultRequest resultRequest, String restIpAddress, int restPort) throws URISyntaxException {
        return new URIBuilder()
                       .setScheme("http")
                       .setHost(restIpAddress)
                       .setPort(restPort)
                       .setPath(QUERY_RESULT_ENDPOINT.replace("*", String.valueOf(resultRequest.getResultId())))
                       .setParameter(METRICS, String.valueOf(resultRequest.isShowMetrics()))
                       .build();
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
}
