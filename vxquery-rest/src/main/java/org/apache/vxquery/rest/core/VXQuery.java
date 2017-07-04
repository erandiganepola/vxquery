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
package org.apache.vxquery.rest.core;

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
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.client.dataset.HyracksDataset;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.control.nc.resources.memory.FrameManager;
import org.apache.hyracks.dataflow.common.comm.io.ResultFrameTupleAccessor;
import org.apache.vxquery.compiler.CompilerControlBlock;
import org.apache.vxquery.compiler.algebricks.VXQueryGlobalDataFactory;
import org.apache.vxquery.compiler.algebricks.prettyprint.VXQueryLogicalExpressionPrettyPrintVisitor;
import org.apache.vxquery.context.DynamicContext;
import org.apache.vxquery.context.DynamicContextImpl;
import org.apache.vxquery.context.RootStaticContextImpl;
import org.apache.vxquery.context.StaticContextImpl;
import org.apache.vxquery.rest.exceptions.VXQueryRuntimeException;
import org.apache.vxquery.rest.request.QueryRequest;
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
import java.net.InetAddress;
import java.nio.file.Files;
import java.util.Date;
import java.util.EnumSet;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.apache.vxquery.rest.Constants.Properties.HYRACKS_CLIENT_IP;
import static org.apache.vxquery.rest.Constants.Properties.HYRACKS_CLIENT_PORT;
import static org.apache.vxquery.rest.Constants.RESULT_URL_PREFIX;

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
    private ExecutorService workers;
    private AtomicLong atomicLong = new AtomicLong(0);

    private Map<Long, QueryResultResponse> resultMap = new ConcurrentHashMap<>();

    private IHyracksClientConnection hyracksClientConnection;
    private HyracksDataset hyracksDataset;
    /**
     * Following two instances are used only if local hyracks cluster is used
     */
    private ClusterControllerService clusterControllerService;
    private NodeControllerService[] nodeControllerServices;

    public VXQuery(VXQueryConfig config) {
        vxQueryConfig = config;
    }

    public synchronized void start() {
        if (!State.STOPPED.equals(state)) {
            throw new IllegalStateException("VXQuery is at state : " + state);
        }

        setState(State.STARTING);

        // TODO: 6/23/17 Check problems with concurrency and increase thread count
        workers = Executors.newSingleThreadExecutor();

        if (System.getProperty(HYRACKS_CLIENT_IP) == null || System.getProperty(HYRACKS_CLIENT_PORT) == null) {
            LOGGER.log(Level.INFO, "Using local hyracks cluster");
            try {
                startLocalHyracks();
            } catch (Exception e) {
                setState(State.STOPPED);
                LOGGER.log(Level.SEVERE, "Error occurred when starting local hyracks", e);
                throw new VXQueryRuntimeException("Unable to start local hyracks", e);
            }
        } else {
            String hyracksClientIp = System.getProperty(HYRACKS_CLIENT_IP);
            int hyracksClientPort = Integer.parseInt(System.getProperty(HYRACKS_CLIENT_PORT));

            try {
                hyracksClientConnection = new HyracksConnection(hyracksClientIp, hyracksClientPort);
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, String.format("Unable to create a hyracks client connection to %s:%d", hyracksClientIp, hyracksClientPort));
                throw new VXQueryRuntimeException("Unable to create a hyracks client connection", e);
            }

            LOGGER.log(Level.INFO, String.format("Using hyracks connection to %s:%d", hyracksClientIp, hyracksClientPort));
        }

        setState(State.STARTED);
        LOGGER.log(Level.INFO, "VXQuery started successfully");
    }

    public synchronized void stop() {
        if (!State.STOPPED.equals(state)) {
            setState(State.STOPPING);
            workers.shutdownNow();

            try {
                stopLocalHyracks();
            } catch (Exception e) {
                setState(State.STARTED);
                LOGGER.log(Level.SEVERE, "Error occurred when stopping VXQuery", e);
                throw new VXQueryRuntimeException("Unable to stop local hyracks", e);
            }

            setState(State.STOPPED);
            LOGGER.log(Level.INFO, "VXQuery stopped successfully");
        } else {
            LOGGER.log(Level.INFO, "VXQuery is already in state : " + state);
        }
    }

    private synchronized void setState(State newState) {
        state = newState;
    }

    /**
     * Creates a new Hyracks connection with the client IP address and port. Then the query is executed.
     *
     * @throws Exception
     */
    public QueryResponse execute(QueryRequest request) {
        if (!State.STARTED.equals(state)) {
            throw new IllegalStateException("VXQuery hasn't started yet");
        }

        QueryResponse response = prepareQueryResponse(request);
        try {
            runQuery(request, response);
            response.setStatus(Status.SUCCESS.toString());
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error occurred when running the query", e);
            response.setStatus(Status.FAILED.toString());
        }
        return response;
    }

    /**
     * Prepares a new {@link QueryResponse} by assigning a new UUID as the request ID. Also the statement will be
     * added to the response.
     *
     * @param request {@link QueryRequest} received
     * @return query response instance to be sent to the caller
     */
    private QueryResponse prepareQueryResponse(QueryRequest request) {
        QueryResponse response = new QueryResponse();
        response.setRequestId(UUID.randomUUID().toString());
        response.setStatement(request.getStatement());
        return response;
    }

    /**
     * Run the statement given in the {@link QueryRequest}
     *
     * @throws Exception
     */
    private void runQuery(QueryRequest request, QueryResponse response) throws Exception {
        String query = request.getStatement();
        response.setStatement(query);

        ResultSetId resultSetId = createResultSetId();
        response.setResultId(resultSetId.getId());
        response.setResultUrl(RESULT_URL_PREFIX + resultSetId.getId());

        Date start;
        Date end;

        // Adding a query compilation listener
        VXQueryCompilationListener listener = new VXQueryCompilationListener(response, request.isShowAbstractSyntaxTree(),
                request.isShowTranslatedExpressionTree(), request.isShowOptimizedExpressionTree(), request.isShowRuntimePlan());

        // Obtaining the node controller information from hyracks client connection
        Map<String, NodeControllerInfo> nodeControllerInfos = hyracksClientConnection.getNodeControllerInfos();

        start = request.isShowMetrics() ? new Date() : null;

        // Compiling the XQuery given
        XMLQueryCompiler compiler = new XMLQueryCompiler(listener, nodeControllerInfos, request.getFrameSize(),
                vxQueryConfig.getAvailableProcessors(), vxQueryConfig.getJoinHashSize(), vxQueryConfig.getMaximumDataSize(), vxQueryConfig.getHdfsConf());
        CompilerControlBlock compilerControlBlock = new CompilerControlBlock(new StaticContextImpl(RootStaticContextImpl.INSTANCE),
                resultSetId, null);
        compiler.compile(null, new StringReader(query), compilerControlBlock, request.getOptimization());

        if (request.isShowMetrics()) {
            end = new Date();
            response.getMetrics().setCompileTime(end.getTime() - start.getTime());
        }

        if (request.isCompileOnly()) {
            return;
        }

        // TODO: 6/30/17 Use futures here
        workers.submit(new Runnable() {
            @Override
            public void run() {
                QueryResultResponse result = new QueryResultResponse();
                result.setRequestId(response.getRequestId());
                result.setStatus(Status.SUCCESS.toString());
                resultMap.put(response.getResultId(), result);

                Module module = compiler.getModule();
                JobSpecification js = module.getHyracksJobSpecification();
                DynamicContext dCtx = new DynamicContextImpl(module.getModuleContext());
                js.setGlobalJobDataFactory(new VXQueryGlobalDataFactory(dCtx.createFactory()));

                long elapsedTime = 0;
                Date start, end;

                // Repeat execution for given times. Defaults to 1 iteration.
                for (int i = 0; i < request.getRepeatExecutions(); ++i) {
                    start = request.isShowMetrics() ? new Date() : null;
                    try {
                        runJob(resultSetId, js);
                    } catch (Exception e) {
                        LOGGER.log(Level.SEVERE, "Error occurred when running job", e);
                    }

                    // Set the elapsed time if requested
                    if (request.isShowMetrics()) {
                        end = new Date();
                        elapsedTime += end.getTime() - start.getTime();
                        result.getMetrics().setElapsedTime(elapsedTime);
                    }
                }
            }
        });
    }

    /**
     * Creates a Hyracks dataset, if not already existing with the job frame size, and 1 reader. Allocates a new buffer
     * of size specified in the frame of Hyracks node. Creates new dataset reader with the current job ID and result
     * set ID. Outputs the string in buffer for each frame.
     *
     * @param spec JobSpecification object, containing frame size. Current specified job.
     * @throws Exception
     */
    private void runJob(ResultSetId resultSetId, JobSpecification spec) throws Exception {
        int nReaders = 1;

        if (hyracksDataset == null) {
            hyracksDataset = new HyracksDataset(hyracksClientConnection, spec.getFrameSize(), nReaders);
        }

        JobId jobId = hyracksClientConnection.startJob(spec, EnumSet.of(JobFlag.PROFILE_RUNTIME));

        FrameManager resultDisplayFrameMgr = new FrameManager(spec.getFrameSize());
        IFrame frame = new VSizeFrame(resultDisplayFrameMgr);
        IHyracksDatasetReader reader = hyracksDataset.createReader(jobId, resultSetId);
        IFrameTupleAccessor frameTupleAccessor = new ResultFrameTupleAccessor();

        // TODO: 7/3/17 Instead of keeping results in RAM, write to temporary files
        OutputStream resultStream = new ByteArrayOutputStream();

        try (PrintWriter writer = new PrintWriter(resultStream, true)) {
            while (reader.read(frame) > 0) {
                writer.print(ResultUtils.getStringFromBuffer(frame.getBuffer(), frameTupleAccessor));
                writer.flush();
                frame.getBuffer().clear();
            }
        }

        hyracksClientConnection.waitForCompletion(jobId);

        resultMap.get(resultSetId.getId()).setResults(resultStream.toString());
        LOGGER.log(Level.INFO, String.format("Result for request %d : %s", resultSetId.getId(), resultStream.toString()));
    }

    /**
     * Returns the query results for a given result set id
     *
     * @param resultId ID of the result set
     * @return query result
     */
    public QueryResultResponse getResult(long resultId) {
        QueryResultResponse response;
        if (resultMap.containsKey(resultId)) {
            if (resultMap.get(resultId).getResults() != null) {
                response = resultMap.get(resultId);
            } else {
                response = new QueryResultResponse();
                response.setStatus(Status.INCOMPLETE.toString());
            }
        } else {
            response = new QueryResultResponse();
            response.setStatus(Status.NOT_FOUND.toString());
        }

        return response;
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

    /**
     * Start local virtual cluster with cluster controller node and node controller nodes. IP address provided for node
     * controller is localhost. Unassigned ports 39000 and 39001 are used for client and cluster port respectively.
     * Creates a new Hyracks connection with the IP address and client ports.
     *
     * @throws Exception
     */
    @SuppressWarnings("Duplicates")
    public void startLocalHyracks() throws Exception {
        String localAddress = InetAddress.getLocalHost().getHostAddress();
        CCConfig ccConfig = new CCConfig();
        ccConfig.clientNetIpAddress = localAddress;
        ccConfig.clientNetPort = 39000;
        ccConfig.clusterNetIpAddress = localAddress;
        ccConfig.clusterNetPort = 39001;
        ccConfig.httpPort = 39002;
        ccConfig.profileDumpPeriod = 10000;
        clusterControllerService = new ClusterControllerService(ccConfig);
        clusterControllerService.start();

        nodeControllerServices = new NodeControllerService[vxQueryConfig.getLocalNodeControllers()];
        for (int i = 0; i < nodeControllerServices.length; i++) {
            NCConfig ncConfig = new NCConfig();
            ncConfig.ccHost = "localhost";
            ncConfig.ccPort = 39001;
            ncConfig.clusterNetIPAddress = localAddress;
            ncConfig.dataIPAddress = localAddress;
            ncConfig.resultIPAddress = localAddress;
            ncConfig.nodeId = "nc" + (i + 1);
            //TODO: enable index folder as a cli option for on-the-fly indexing queries
            ncConfig.ioDevices = Files.createTempDirectory(ncConfig.nodeId).toString();
            nodeControllerServices[i] = new NodeControllerService(ncConfig);
            nodeControllerServices[i].start();
        }

        hyracksClientConnection = new HyracksConnection(ccConfig.clientNetIpAddress, ccConfig.clientNetPort);
    }

    /**
     * Shuts down the virtual cluster, along with all nodes and node execution, network and queue managers.
     *
     * @throws Exception
     */
    public void stopLocalHyracks() throws Exception {
        for (int i = 0; i < nodeControllerServices.length; i++) {
            nodeControllerServices[i].stop();
        }
        clusterControllerService.stop();
    }

    public State getState() {
        return state;
    }

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
                    LOGGER.log(Level.SEVERE, "Error occurred when obtaining runtime plan from job specification : " + jobSpec.toString(), e);
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
                LOGGER.log(Level.SEVERE, "Error occurred when pretty printing expression : " + e.getMessage());
            }
            return sb;
        }
    }
}
