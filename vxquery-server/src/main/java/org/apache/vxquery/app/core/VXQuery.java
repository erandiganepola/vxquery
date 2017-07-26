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
package org.apache.vxquery.app.core;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.DomDriver;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.AlgebricksAppendable;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.LogicalOperatorPrettyPrintVisitor;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.PlanPrettyPrinter;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionVisitor;
import org.apache.hyracks.api.client.HyracksConnection;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.client.NodeControllerInfo;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.dataset.IHyracksDatasetReader;
import org.apache.hyracks.api.dataset.ResultSetId;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.client.dataset.HyracksDataset;
import org.apache.hyracks.control.nc.resources.memory.FrameManager;
import org.apache.hyracks.dataflow.common.comm.io.ResultFrameTupleAccessor;
import org.apache.vxquery.compiler.CompilerControlBlock;
import org.apache.vxquery.compiler.algebricks.VXQueryGlobalDataFactory;
import org.apache.vxquery.compiler.algebricks.prettyprint.VXQueryLogicalExpressionPrettyPrintVisitor;
import org.apache.vxquery.context.DynamicContext;
import org.apache.vxquery.context.DynamicContextImpl;
import org.apache.vxquery.context.RootStaticContextImpl;
import org.apache.vxquery.context.StaticContextImpl;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.rest.Constants;
import org.apache.vxquery.rest.exceptions.VXQueryRuntimeException;
import org.apache.vxquery.rest.request.QueryRequest;
import org.apache.vxquery.rest.request.QueryResultRequest;
import org.apache.vxquery.rest.response.APIResponse;
import org.apache.vxquery.rest.response.Error;
import org.apache.vxquery.rest.response.QueryResponse;
import org.apache.vxquery.rest.response.QueryResultResponse;
import org.apache.vxquery.result.ResultUtils;
import org.apache.vxquery.xmlquery.ast.ModuleNode;
import org.apache.vxquery.xmlquery.query.Module;
import org.apache.vxquery.xmlquery.query.XMLQueryCompiler;
import org.apache.vxquery.xmlquery.query.XQueryCompilationListener;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.util.Date;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.util.logging.Level.SEVERE;
import static org.apache.vxquery.rest.Constants.ErrorCodes.NOT_FOUND;
import static org.apache.vxquery.rest.Constants.ErrorCodes.PROBLEM_WITH_QUERY;
import static org.apache.vxquery.rest.Constants.ErrorCodes.UNFORSEEN_PROBLEM;

/**
 * Main class responsible for handling query requests. This class will first compile, then submit query to hyracks and
 * finally fetch results for a given query.
 *
 * @author Erandi Ganepola
 */
public class VXQuery {

    public static final Logger LOGGER = Logger.getLogger(VXQuery.class.getName());

    private volatile State state = State.STOPPED;
    private VXQueryConfig vxQueryConfig;
    private AtomicLong atomicLong = new AtomicLong(0);

    private Map<Long, HyracksJobContext> jobContexts = new ConcurrentHashMap<>();

    private IHyracksClientConnection hyracksClientConnection;
    private HyracksDataset hyracksDataset;

    public VXQuery(VXQueryConfig config) {
        vxQueryConfig = config;
    }

    /**
     * Starts VXQuery class by creating a {@link IHyracksClientConnection} which will later be used to submit and
     * retrieve queries and results to/from hyracks.
     */
    public synchronized void start() {
        if (!State.STOPPED.equals(state)) {
            throw new IllegalStateException("VXQuery is at state : " + state);
        }

        if (vxQueryConfig.getHyracksClientIp() == null) {
            throw new IllegalArgumentException("hyracksClientIp is required to connect to hyracks");
        }

        setState(State.STARTING);

        try {
            hyracksClientConnection = new HyracksConnection(vxQueryConfig.getHyracksClientIp(), vxQueryConfig.getHyracksClientPort());
        } catch (Exception e) {
            LOGGER.log(SEVERE, String.format("Unable to create a hyracks client connection to %s:%d", vxQueryConfig.getHyracksClientIp(), vxQueryConfig.getHyracksClientPort()));
            throw new VXQueryRuntimeException("Unable to create a hyracks client connection", e);
        }

        LOGGER.log(Level.FINE, String.format("Using hyracks connection to %s:%d", vxQueryConfig.getHyracksClientIp(), vxQueryConfig.getHyracksClientPort()));

        setState(State.STARTED);
        LOGGER.log(Level.INFO, "VXQuery started successfully");
    }

    private synchronized void setState(State newState) {
        state = newState;
    }

    /**
     * Submits a query to hyracks to be run after compiling. Required intermediate results and metrics are also
     * calculated according to the {@link QueryRequest}. Checks if this class has started before moving further.
     *
     * @param request {@link QueryRequest} containing information about the query to be executed and the merics required
     *                along with the results
     * @return QueryResponse if no error occurs | ErrorResponse else
     */
    public APIResponse execute(final QueryRequest request) {
        if (!State.STARTED.equals(state)) {
            throw new IllegalStateException("VXQuery is at state : " + state);
        }

        QueryResponse response = APIResponse.newQueryResponse(request.getRequestId());

        String query = request.getStatement();
        response.setStatement(query);

        final ResultSetId resultSetId = createResultSetId();
        response.setResultId(resultSetId.getId());
        response.setResultUrl(Constants.RESULT_URL_PREFIX + resultSetId.getId());

        Date start;
        Date end;

        // Adding a query compilation listener
        VXQueryCompilationListener listener = new VXQueryCompilationListener(response,
                                                                                    request.isShowAbstractSyntaxTree(),
                                                                                    request.isShowTranslatedExpressionTree(),
                                                                                    request.isShowOptimizedExpressionTree(),
                                                                                    request.isShowRuntimePlan());

        // Obtaining the node controller information from hyracks client connection
        Map<String, NodeControllerInfo> nodeControllerInfos = null;
        try {
            nodeControllerInfos = hyracksClientConnection.getNodeControllerInfos();
        } catch (HyracksException e) {
            LOGGER.log(Level.SEVERE, String.format("Error occurred when obtaining NC info: '%s'", e.getMessage()));
            return APIResponse.newErrorResponse(request.getRequestId(), Error.builder()
                                                                                .withCode(UNFORSEEN_PROBLEM)
                                                                                .withMessage("Hyracks connection problem: " + e.getMessage())
                                                                                .build());
        }

        start = request.isShowMetrics() ? new Date() : null;

        // Compiling the XQuery given
        final XMLQueryCompiler compiler = new XMLQueryCompiler(listener, nodeControllerInfos,
                                                                      request.getFrameSize(),
                                                                      vxQueryConfig.getAvailableProcessors(),
                                                                      vxQueryConfig.getJoinHashSize(),
                                                                      vxQueryConfig.getMaximumDataSize(),
                                                                      vxQueryConfig.getHdfsConf());
        CompilerControlBlock compilerControlBlock = new CompilerControlBlock(new StaticContextImpl(RootStaticContextImpl.INSTANCE),
                                                                                    resultSetId, null);
        try {
            compiler.compile(null, new StringReader(query), compilerControlBlock, request.getOptimization(), null);
        } catch (AlgebricksException e) {
            LOGGER.log(Level.SEVERE, String.format("Error occurred when compiling query: '%s' with message: '%s'", query, e.getMessage()));
            return APIResponse.newErrorResponse(request.getRequestId(), Error.builder()
                                                                                .withCode(PROBLEM_WITH_QUERY)
                                                                                .withMessage("Query compilation failure: " + e.getMessage())
                                                                                .build());
        } catch (SystemException e) {
            LOGGER.log(Level.SEVERE, String.format("Error occurred when compiling query: '%s' with message: '%s'", query, e.getMessage()));
            return APIResponse.newErrorResponse(request.getRequestId(), Error.builder()
                                                                                .withCode(PROBLEM_WITH_QUERY)
                                                                                .withMessage("Query compilation failure: " + e.getMessage())
                                                                                .build());
        }

        if (request.isShowMetrics()) {
            end = new Date();
            response.getMetrics().setCompileTime(end.getTime() - start.getTime());
        }

        if (!request.isCompileOnly()) {
            Module module = compiler.getModule();
            JobSpecification js = module.getHyracksJobSpecification();
            DynamicContext dCtx = new DynamicContextImpl(module.getModuleContext());
            js.setGlobalJobDataFactory(new VXQueryGlobalDataFactory(dCtx.createFactory()));

            try {
                JobId jobId = hyracksClientConnection.startJob(js, EnumSet.of(JobFlag.PROFILE_RUNTIME));
                jobContexts.put(resultSetId.getId(), new HyracksJobContext(jobId, js.getFrameSize(), resultSetId));
            } catch (Exception e) {
                LOGGER.log(SEVERE, "Error occurred when submitting job to hyracks for query: " + query, e);
                return APIResponse.newErrorResponse(request.getRequestId(), Error.builder()
                                                                                    .withCode(UNFORSEEN_PROBLEM)
                                                                                    .withMessage("Error occurred when starting hyracks job")
                                                                                    .build());
            }
        }
        return response;
    }

    // TODO: 7/12/17 Allow multiple executions of the same query

    /**
     * Returns the query results for a given result set id.
     *
     * @param request {@link QueryResultRequest} with result ID required
     * @return Either a {@link QueryResultResponse} if no error occurred | {@link org.apache.vxquery.rest.response.ErrorResponse}
     * else.
     */
    public APIResponse getResult(QueryResultRequest request) {
        if (jobContexts.containsKey(request.getResultId())) {
            QueryResultResponse resultResponse = APIResponse.newQueryResultResponse(request.getRequestId());
            try {
                readResults(jobContexts.get(request.getResultId()), resultResponse);
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Error occurred when reading results for id : " + request.getResultId());
                return APIResponse.newErrorResponse(request.getRequestId(), Error.builder()
                                                                                    .withCode(UNFORSEEN_PROBLEM)
                                                                                    .withMessage("Error occurred when reading results from hyracks for result ID: " + request.getResultId())
                                                                                    .build());
            }
            return resultResponse;
        } else {
            return APIResponse.newErrorResponse(request.getRequestId(), Error.builder()
                                                                                .withCode(NOT_FOUND)
                                                                                .withMessage("No query found for result ID : " + request.getResultId())
                                                                                .build());
        }
    }

    /**
     * Reads results from hyracks given the {@link HyracksJobContext} containing {@link ResultSetId} and {@link JobId}
     * mapping.
     *
     * @param jobContext     mapoing between the {@link ResultSetId} and corresponding hyracks {@link JobId}
     * @param resultResponse {@link QueryResultResponse} object to which the result will be added.
     * @throws Exception IOErrors and etc
     */
    private void readResults(HyracksJobContext jobContext, QueryResultResponse resultResponse) throws Exception {
        int nReaders = 1;

        if (hyracksDataset == null) {
            hyracksDataset = new HyracksDataset(hyracksClientConnection, jobContext.getFrameSize(), nReaders);
        }

        FrameManager resultDisplayFrameMgr = new FrameManager(jobContext.getFrameSize());
        IFrame frame = new VSizeFrame(resultDisplayFrameMgr);
        IHyracksDatasetReader reader = hyracksDataset.createReader(jobContext.getJobId(), jobContext.getResultSetId());
        IFrameTupleAccessor frameTupleAccessor = new ResultFrameTupleAccessor();

        OutputStream resultStream = new ByteArrayOutputStream();

        try (PrintWriter writer = new PrintWriter(resultStream, true)) {
            while (reader.read(frame) > 0) {
                writer.print(ResultUtils.getStringFromBuffer(frame.getBuffer(), frameTupleAccessor));
                writer.flush();
                frame.getBuffer().clear();
            }
        }

        // TODO: 7/12/17 Set metrics
        hyracksClientConnection.waitForCompletion(jobContext.getJobId());
        LOGGER.log(Level.INFO, String.format("Result for resultId %d completed", jobContext.getResultSetId().getId()));
        resultResponse.setResults(resultStream.toString());
    }

    /**
     * Create a unique result set id to get the correct query back from the cluster.
     *
     * @return Result Set id generated with current system time.
     */
    protected ResultSetId createResultSetId() {
        long resultSetId = atomicLong.incrementAndGet();
        LOGGER.log(Level.FINE, String.format("Creating result set with ID : %d", resultSetId));
        return new ResultSetId(resultSetId);
    }

    public synchronized void stop() {
        if (!State.STOPPED.equals(state)) {
            setState(State.STOPPING);
            LOGGER.log(Level.FINE, "Stooping VXQuery");
            setState(State.STOPPED);
            LOGGER.log(Level.INFO, "VXQuery stopped successfully");
        } else {
            LOGGER.log(Level.INFO, "VXQuery is already in state : " + state);
        }
    }

    public State getState() {
        return state;
    }

    /**
     * A {@link XQueryCompilationListener} implementation to be used to add AbstractSyntaxTree, RuntimePlan and etc to
     * the {@link QueryResponse} if requested by the user.
     */
    private class VXQueryCompilationListener implements XQueryCompilationListener {
        private QueryResponse response;
        private boolean showAbstractSyntaxTree;
        private boolean showTranslatedExpressionTree;
        private boolean showOptimizedExpressionTree;
        private boolean showRuntimePlan;

        public VXQueryCompilationListener(QueryResponse response,
                                          boolean showAbstractSyntaxTree,
                                          boolean showTranslatedExpressionTree,
                                          boolean showOptimizedExpressionTree,
                                          boolean showRuntimePlan) {
            this.response = response;
            this.showAbstractSyntaxTree = showAbstractSyntaxTree;
            this.showTranslatedExpressionTree = showTranslatedExpressionTree;
            this.showOptimizedExpressionTree = showOptimizedExpressionTree;
            this.showRuntimePlan = showRuntimePlan;
        }

        @Override
        public void notifyParseResult(ModuleNode moduleNode) {
            if (showAbstractSyntaxTree) {
                response.setAbstractSyntaxTree(new XStream(new DomDriver()).toXML(moduleNode));
            }
        }

        @Override
        public void notifyTranslationResult(Module module) {
            if (showTranslatedExpressionTree) {
                response.setTranslatedExpressionTree(appendPrettyPlan(new StringBuilder(), module).toString());
            }
        }

        @Override
        public void notifyTypecheckResult(Module module) {
        }

        @Override
        public void notifyCodegenResult(Module module) {
            if (showRuntimePlan) {
                JobSpecification jobSpec = module.getHyracksJobSpecification();
                try {
                    response.setRuntimePlan(jobSpec.toJSON().toString());
                } catch (IOException e) {
                    LOGGER.log(SEVERE, "Error occurred when obtaining runtime plan from job specification : " + jobSpec.toString(), e);
                }
            }
        }

        @Override
        public void notifyOptimizedResult(Module module) {
            if (showOptimizedExpressionTree) {
                response.setOptimizedExpressionTree(appendPrettyPlan(new StringBuilder(), module).toString());
            }
        }

        @SuppressWarnings("Duplicates")
        private StringBuilder appendPrettyPlan(StringBuilder sb, Module module) {
            try {
                ILogicalExpressionVisitor<String, Integer> ev = new VXQueryLogicalExpressionPrettyPrintVisitor(
                                                                                                                      module.getModuleContext());
                AlgebricksAppendable buffer = new AlgebricksAppendable();
                LogicalOperatorPrettyPrintVisitor v = new LogicalOperatorPrettyPrintVisitor(buffer, ev);
                PlanPrettyPrinter.printPlan(module.getBody(), v, 0);
                sb.append(buffer.toString());
            } catch (AlgebricksException e) {
                LOGGER.log(SEVERE, "Error occurred when pretty printing expression : " + e.getMessage());
            }
            return sb;
        }
    }
}
