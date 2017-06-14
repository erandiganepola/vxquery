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
package org.apache.vxquery.rest;

import org.apache.hyracks.api.client.HyracksConnection;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.client.NodeControllerInfo;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.dataset.IHyracksDataset;
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
import org.apache.vxquery.context.DynamicContext;
import org.apache.vxquery.context.DynamicContextImpl;
import org.apache.vxquery.context.RootStaticContextImpl;
import org.apache.vxquery.context.StaticContextImpl;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.rest.request.QueryRequest;
import org.apache.vxquery.rest.response.QueryResponse;
import org.apache.vxquery.result.ResultUtils;
import org.apache.vxquery.xmlquery.query.Module;
import org.apache.vxquery.xmlquery.query.VXQueryCompilationListener;
import org.apache.vxquery.xmlquery.query.XMLQueryCompiler;

import java.io.*;
import java.net.InetAddress;
import java.nio.file.Files;
import java.util.*;

@SuppressWarnings("Duplicates")
public class VXQuery {

    private final QueryRequest request;
    private final QueryResponse response;

    private ClusterControllerService cc;
    private NodeControllerService[] ncs;
    private IHyracksClientConnection hcc;
    private IHyracksDataset hds;

    private ResultSetId resultSetId;
    private static List<String> timingMessages = new ArrayList<>();
    private static long sumTiming;
    private static long sumSquaredTiming;
    private static long minTiming = Long.MAX_VALUE;
    private static long maxTiming = Long.MIN_VALUE;

    /**
     * Constructor to use command line options passed.
     *
     * @param request Query request to be processed
     */
    public VXQuery(QueryRequest request, QueryResponse response) {
        this.request = request;
        this.response = response;
    }

    /**
     * Creates a new Hyracks connection with: the client IP address and port provided, if IP address is provided in command line. Otherwise create a new virtual
     * cluster with Hyracks nodes. Queries passed are run either way. After running queries, if a virtual cluster has been created, it is shut down.
     *
     * @throws Exception
     */
    public void execute() throws Exception {
        System.setProperty("vxquery.buffer_size", Integer.toString(request.getBufferSize()));

        if (request.getClientNetIpAddress() != null) {
            hcc = new HyracksConnection(request.getClientNetIpAddress(), request.getClientNetPort());
            runQueries();
        } else {
            if (!request.isCompileOnly()) {
                startLocalHyracks();
            }
            try {
                runQueries();
            } finally {
                if (!request.isCompileOnly()) {
                    stopLocalHyracks();
                }
            }
        }
    }

    /**
     * Reads the contents of the files passed in the list of arguments to a string. If -showquery argument is passed, output the query as string. Run the query
     * for the string.
     *
     * @throws IOException
     * @throws SystemException
     * @throws Exception
     */
    private void runQueries() throws Exception {
        Date start;
        Date end;

        String query = request.getStatement();
        if (request.isShowQuery()) {
            System.err.println(query);
        }

        VXQueryCompilationListener listener = new VXQueryCompilationListener(request.isShowAbstractSyntaxTree(),
                request.isShowTranslatedExpressionTree(), request.isShowOptimizedExpressionTree(), request.isShowRuntimePlan());

        start = request.isShowMetrics() ? new Date() : null;

        Map<String, NodeControllerInfo> nodeControllerInfos = null;
        if (hcc != null) {
            nodeControllerInfos = hcc.getNodeControllerInfos();
        }
        XMLQueryCompiler compiler = new XMLQueryCompiler(listener, nodeControllerInfos, request.getFrameSize(),
                request.getAvailableProcessors(), request.getJoinHashSize(), request.getMaximumDataSize(), request.getHdfsConf());
        resultSetId = createResultSetId();
        CompilerControlBlock ccb = new CompilerControlBlock(new StaticContextImpl(RootStaticContextImpl.INSTANCE),
                resultSetId, null);
        compiler.compile(null, new StringReader(query), ccb, request.getOptimizationLevel());
        // if -timing argument passed, show the starting and ending times
        if (request.isShowMetrics()) {
            end = new Date();
            timingMessage("Compile time: " + (end.getTime() - start.getTime()) + " ms");
        }
        if (request.isCompileOnly()) {
            return;
        }

        Module module = compiler.getModule();
        JobSpecification js = module.getHyracksJobSpecification();

        DynamicContext dCtx = new DynamicContextImpl(module.getModuleContext());
        js.setGlobalJobDataFactory(new VXQueryGlobalDataFactory(dCtx.createFactory()));

        OutputStream resultStream = System.out;
        if (request.getResultFile() != null) {
            resultStream = new FileOutputStream(new File(request.getResultFile()));
        }

        PrintWriter writer = new PrintWriter(resultStream, true);
        // Repeat execution for number of times provided in -repeatexec argument
        for (int i = 0; i < request.getRepeatExecutions(); ++i) {
            start = request.isShowMetrics() ? new Date() : null;
            runJob(js, writer);
            // if -timing argument passed, show the starting and ending times
            if (request.isShowMetrics()) {
                end = new Date();
                long currentRun = end.getTime() - start.getTime();
                if ((i + 1) > request.getTimingIgnoreQueries()) {
                    sumTiming += currentRun;
                    sumSquaredTiming += currentRun * currentRun;
                    if (currentRun < minTiming) {
                        minTiming = currentRun;
                    }
                    if (maxTiming < currentRun) {
                        maxTiming = currentRun;
                    }
                }
                timingMessage("Job (" + (i + 1) + ") execution time: " + currentRun + " ms");
            }
        }
    }

    /**
     * Creates a Hyracks dataset, if not already existing with the job frame size, and 1 reader. Allocates a new buffer of size specified in the frame of Hyracks
     * node. Creates new dataset reader with the current job ID and result set ID. Outputs the string in buffer for each frame.
     *
     * @param spec   JobSpecification object, containing frame size. Current specified job.
     * @param writer Writer for output of job.
     * @throws Exception
     */
    private void runJob(JobSpecification spec, PrintWriter writer) throws Exception {
        int nReaders = 1;
        if (hds == null) {
            hds = new HyracksDataset(hcc, spec.getFrameSize(), nReaders);
        }

        JobId jobId = hcc.startJob(spec, EnumSet.of(JobFlag.PROFILE_RUNTIME));

        FrameManager resultDisplayFrameMgr = new FrameManager(spec.getFrameSize());
        IFrame frame = new VSizeFrame(resultDisplayFrameMgr);
        IHyracksDatasetReader reader = hds.createReader(jobId, resultSetId);
        IFrameTupleAccessor frameTupleAccessor = new ResultFrameTupleAccessor();

        while (reader.read(frame) > 0) {
            writer.print(ResultUtils.getStringFromBuffer(frame.getBuffer(), frameTupleAccessor));
            writer.flush();
            frame.getBuffer().clear();
        }

        hcc.waitForCompletion(jobId);
    }

    /**
     * Create a unique result set id to get the correct query back from the cluster.
     *
     * @return Result Set id generated with current system time.
     */
    protected ResultSetId createResultSetId() {
        return new ResultSetId(System.nanoTime());
    }

    /**
     * Start local virtual cluster with cluster controller node and node controller nodes. IP address provided for node controller is localhost. Unassigned ports
     * 39000 and 39001 are used for client and cluster port respectively. Creates a new Hyracks connection with the IP address and client ports.
     *
     * @throws Exception
     */
    public void startLocalHyracks() throws Exception {
        String localAddress = InetAddress.getLocalHost().getHostAddress();
        CCConfig ccConfig = new CCConfig();
        ccConfig.clientNetIpAddress = localAddress;
        ccConfig.clientNetPort = 39000;
        ccConfig.clusterNetIpAddress = localAddress;
        ccConfig.clusterNetPort = 39001;
        ccConfig.httpPort = 39002;
        ccConfig.profileDumpPeriod = 10000;
        cc = new ClusterControllerService(ccConfig);
        cc.start();

        ncs = new NodeControllerService[request.getLocalNodeControllers()];
        for (int i = 0; i < ncs.length; i++) {
            NCConfig ncConfig = new NCConfig();
            ncConfig.ccHost = "localhost";
            ncConfig.ccPort = 39001;
            ncConfig.clusterNetIPAddress = localAddress;
            ncConfig.dataIPAddress = localAddress;
            ncConfig.resultIPAddress = localAddress;
            ncConfig.nodeId = "nc" + (i + 1);
            //TODO: enable index folder as a cli option for on-the-fly indexing queries
            ncConfig.ioDevices = Files.createTempDirectory(ncConfig.nodeId).toString();
            ncs[i] = new NodeControllerService(ncConfig);
            ncs[i].start();
        }

        hcc = new HyracksConnection(ccConfig.clientNetIpAddress, ccConfig.clientNetPort);
    }

    /**
     * Shuts down the virtual cluster, along with all nodes and node execution, network and queue managers.
     *
     * @throws Exception
     */
    public void stopLocalHyracks() throws Exception {
        for (int i = 0; i < ncs.length; i++) {
            ncs[i].stop();
        }
        cc.stop();
    }

    /**
     * Save and print out the timing message.
     *
     * @param message
     */
    private static void timingMessage(String message) {
        System.out.println(message);
        timingMessages.add(message);
    }
}
