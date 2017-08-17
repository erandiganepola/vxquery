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
package org.apache.vxquery.rest.request;

import org.apache.vxquery.rest.RestServer;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * Request to represent a query request coming to the {@link RestServer}
 *
 * @author Erandi Ganepola
 */
public class QueryRequest {

    private String statement;
    private boolean async = true;
    private boolean compileOnly;
    private int optimization = 0;
    /** Frame size in bytes. (default: 65,536) */
    private int frameSize = 65536;
    private int repeatExecutions = 1;
    private boolean showMetrics = false;
    private boolean showAbstractSyntaxTree = false;
    private boolean showTranslatedExpressionTree = false;
    private boolean showOptimizedExpressionTree = false;
    private boolean showRuntimePlan = false;
    /** A unique UUID to uniquely identify a given request */
    private String requestId;

    /** An optional map of source files. Required for XTests */
    private Map<String, File> sourceFileMap = new HashMap<>();

    public QueryRequest(String statement) {
        this(null, statement);
    }

    public QueryRequest(String requestId, String statement) {
        if (statement == null) {
            throw new IllegalArgumentException("Statement cannot be null");
        }

        this.statement = statement;
        this.requestId = requestId;
    }

    public String getStatement() {
        return statement;
    }

    public boolean isCompileOnly() {
        return compileOnly;
    }

    public void setCompileOnly(boolean compileOnly) {
        this.compileOnly = compileOnly;
    }

    public int getOptimization() {
        return optimization;
    }

    public void setOptimization(int optimization) {
        this.optimization = optimization;
    }

    public int getFrameSize() {
        return frameSize;
    }

    public void setFrameSize(int frameSize) {
        this.frameSize = frameSize;
    }

    public int getRepeatExecutions() {
        return repeatExecutions;
    }

    public void setRepeatExecutions(int repeatExecutions) {
        this.repeatExecutions = repeatExecutions;
    }

    public boolean isShowAbstractSyntaxTree() {
        return showAbstractSyntaxTree;
    }

    public void setShowAbstractSyntaxTree(boolean showAbstractSyntaxTree) {
        this.showAbstractSyntaxTree = showAbstractSyntaxTree;
    }

    public boolean isShowTranslatedExpressionTree() {
        return showTranslatedExpressionTree;
    }

    public void setShowTranslatedExpressionTree(boolean showTranslatedExpressionTree) {
        this.showTranslatedExpressionTree = showTranslatedExpressionTree;
    }

    public boolean isShowOptimizedExpressionTree() {
        return showOptimizedExpressionTree;
    }

    public void setShowOptimizedExpressionTree(boolean showOptimizedExpressionTree) {
        this.showOptimizedExpressionTree = showOptimizedExpressionTree;
    }

    public boolean isShowRuntimePlan() {
        return showRuntimePlan;
    }

    public void setShowRuntimePlan(boolean showRuntimePlan) {
        this.showRuntimePlan = showRuntimePlan;
    }

    public boolean isShowMetrics() {
        return showMetrics;
    }

    public void setShowMetrics(boolean showMetrics) {
        this.showMetrics = showMetrics;
    }

    public String toString() {
        return String.format("{ statement : %s }", statement);
    }

    public String getRequestId() {
        return requestId;
    }

    public boolean isAsync() {
        return async;
    }

    public void setAsync(boolean async) {
        this.async = async;
    }

    public Map<String, File> getSourceFileMap() {
        return sourceFileMap;
    }

    public void setSourceFileMap(Map<String, File> sourceFileMap) {
        this.sourceFileMap = sourceFileMap;
    }
}
