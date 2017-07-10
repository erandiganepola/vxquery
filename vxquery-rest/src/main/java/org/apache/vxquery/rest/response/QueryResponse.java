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

import javax.xml.bind.annotation.XmlRootElement;

/**
 * Resource class to represent a response to a given user query
 *
 * @author Erandi Ganepola
 */
@XmlRootElement
public class QueryResponse extends APIResponse{

    private long resultId;
    private String requestId;
    private String status;
    private String resultUrl;
    private String statement;
    private String abstractSyntaxTree;
    private String translatedExpressionTree;
    private String optimizedExpressionTree;
    private String runtimePlan;
    private Metrics metrics = new Metrics();

    public long getResultId() {
        return resultId;
    }

    public void setResultId(long resultId) {
        this.resultId = resultId;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public String getResultUrl() {
        return resultUrl;
    }

    public void setResultUrl(String resultUrl) {
        this.resultUrl = resultUrl;
    }

    public String getStatement() {
        return statement;
    }

    public void setStatement(String statement) {
        this.statement = statement;
    }

    public String getAbstractSyntaxTree() {
        return abstractSyntaxTree;
    }

    public void setAbstractSyntaxTree(String abstractSyntaxTree) {
        this.abstractSyntaxTree = abstractSyntaxTree;
    }

    public String getTranslatedExpressionTree() {
        return translatedExpressionTree;
    }

    public void setTranslatedExpressionTree(String translatedExpressionTree) {
        this.translatedExpressionTree = translatedExpressionTree;
    }

    public String getOptimizedExpressionTree() {
        return optimizedExpressionTree;
    }

    public void setOptimizedExpressionTree(String optimizedExpressionTree) {
        this.optimizedExpressionTree = optimizedExpressionTree;
    }

    public String getRuntimePlan() {
        return runtimePlan;
    }

    public void setRuntimePlan(String runtimePlan) {
        this.runtimePlan = runtimePlan;
    }

    public Metrics getMetrics() {
        return metrics;
    }

    public void setMetrics(Metrics metrics) {
        this.metrics = metrics;
    }
}
