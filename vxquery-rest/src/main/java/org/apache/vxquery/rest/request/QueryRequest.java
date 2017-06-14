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

import java.util.HashMap;
import java.util.Map;

/**
 * Request to represent a query request coming to the {@link org.apache.vxquery.rest.RestServer}
 *
 * @author Erandi Ganepola
 */
public class QueryRequest {

    private String statement;

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

    /** Number of available processors. (default: java's available processors) */
    private int availableProcessors = -1;
    /** IP Address of the ClusterController. */
    private String clientNetIpAddress = null;
    /** Port of the ClusterController. (default: 1098) */
    private int clientNetPort = 1098;
    /** Number of local node controllers. (default: 1) */
    private int localNodeControllers = 1;
    /** Join hash size in bytes. (default: 67,108,864) */
    private long joinHashSize = -1;
    /** Maximum possible data size in bytes. (default: 150,323,855,000) */
    private long maximumDataSize = -1;
    /** Disk read buffer size in bytes. */
    private int bufferSize = -1;
    /** Optimization Level. (default: Full Optimization) */
    private int optimizationLevel = Integer.MAX_VALUE;
    /** Show query string */
    private boolean showQuery;
    /** File path to save the query result */
    private String resultFile = "/tmp/result.txt";
    /** Ignore the first X number of quereies */
    private int timingIgnoreQueries = 2;
    /** Bind an external variable */
    private Map<String, String> bindings = new HashMap<>();
    /** Directory path to Hadoop configuration files */
    private String hdfsConf = null;

    public String getStatement() {
        return statement;
    }

    public void setStatement(String statement) {
        this.statement = statement;
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

    public int getAvailableProcessors() {
        return availableProcessors;
    }

    public String getClientNetIpAddress() {
        return clientNetIpAddress;
    }

    public int getClientNetPort() {
        return clientNetPort;
    }

    public int getLocalNodeControllers() {
        return localNodeControllers;
    }

    public long getJoinHashSize() {
        return joinHashSize;
    }

    public long getMaximumDataSize() {
        return maximumDataSize;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public int getOptimizationLevel() {
        return optimizationLevel;
    }

    public boolean isShowQuery() {
        return showQuery;
    }

    public String getResultFile() {
        return resultFile;
    }

    public int getTimingIgnoreQueries() {
        return timingIgnoreQueries;
    }

    public Map<String, String> getBindings() {
        return bindings;
    }

    public String getHdfsConf() {
        return hdfsConf;
    }
}
