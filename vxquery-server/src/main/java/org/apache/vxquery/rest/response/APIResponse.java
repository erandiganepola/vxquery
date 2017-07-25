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

package org.apache.vxquery.rest.response;

import org.apache.vxquery.app.core.Status;

public class APIResponse {

    private String status;
    private String requestId;

    public APIResponse() {
        status = Status.SUCCESS.toString();
    }

    public APIResponse(String status) {
        this.status = status;
    }

    public String getStatus() {
        return status;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public static ErrorResponse newErrorResponse(String requestId, Error error) {
        ErrorResponse response = new ErrorResponse();
        response.setRequestId(requestId);
        response.setError(error);
        return response;
    }

    public static QueryResponse newQueryResponse(String requestId) {
        QueryResponse response = new QueryResponse();
        response.setRequestId(requestId);
        return response;
    }

    public static QueryResultResponse newQueryResultResponse(String requestId) {
        QueryResultResponse response = new QueryResultResponse();
        response.setRequestId(requestId);
        return response;
    }
}
