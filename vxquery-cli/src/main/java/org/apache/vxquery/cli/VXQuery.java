/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.vxquery.cli;

import org.apache.commons.io.FileUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.HttpClientUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.vxquery.app.VXQueryApplication;
import org.apache.vxquery.app.util.RestUtils;
import org.apache.vxquery.rest.request.QueryRequest;
import org.apache.vxquery.rest.request.QueryResultRequest;
import org.apache.vxquery.rest.response.Error;
import org.apache.vxquery.rest.response.ErrorResponse;
import org.apache.vxquery.rest.response.QueryResponse;
import org.apache.vxquery.rest.response.QueryResultResponse;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.LogManager;

import static org.apache.vxquery.core.Constants.HttpHeaderValues.CONTENT_TYPE_JSON;


public class VXQuery {

    private final CmdLineOptions opts;

    private static List<String> timingMessages = new ArrayList<>();
    private static ClusterControllerService clusterControllerService;
    private static NodeControllerService nodeControllerService;

    private String restIpAddress;
    private int restPort;

    /**
     * Constructor to use command line options passed.
     *
     * @param opts Command line options object
     */
    public VXQuery(CmdLineOptions opts) {
        this.opts = opts;
    }

    /**
     * Main method to get command line options and execute query process.
     *
     * @param args command line arguments
     */
    public static void main(String[] args) {
        LogManager.getLogManager().reset();

        final CmdLineOptions opts = new CmdLineOptions();
        CmdLineParser parser = new CmdLineParser(opts);

        // parse command line options, give error message if no arguments passed
        try {
            parser.parseArgument(args);
        } catch (Exception e) {
            parser.printUsage(System.err);
            return;
        }

        if (opts.xqFiles.isEmpty()) {
            parser.printUsage(System.err);
            return;
        }

        VXQuery vxq = new VXQuery(opts);
        vxq.execute(opts.xqFiles);
    }

    private void execute(List<String> xqFiles) {
        if (opts.restIpAddress == null) {
            System.out.println("No REST Ip address given. Creating a local hyracks cluster");
            try {
                restIpAddress = startLocalHyracks();
                restPort = opts.restPort;
            } catch (Exception e) {
                System.err.println("Unable to start local hyracks cluster due to: " + e.getMessage());
                return;
            }
        } else {
            restIpAddress = opts.restIpAddress;
            restPort = opts.restPort;
        }

        System.out.println("Running queries given in: " + Arrays.toString(xqFiles.toArray()));
        runQueries(xqFiles);

        if (opts.restIpAddress == null) {
            try {
                stopLocalHyracks();
            } catch (Exception e) {
                System.err.println("Error occurred when stopping local hyracks: " + e.getMessage());
            }
        }
    }

    public void runQueries(List<String> xqFiles) {
        for (String xqFile : xqFiles) {
            String query;
            try {
                query = slurp(xqFile);
            } catch (IOException e) {
                System.err.println(String.format("Error occurred when reading XQuery file %s with message: %s", xqFile, e.getMessage()));
                continue;
            }

            QueryRequest request = createQueryRequest(opts, query);
            sendQueryRequest(xqFile, request, this);
        }
    }

    private void onQuerySubmitSuccess(String xqFile, QueryRequest request, QueryResponse response) {
        if (response == null) {
            System.err.println(String.format("Unable to execute query %s", request.getStatement()));
            return;
        }

        System.out.println();
        System.out.println("====================================================");
        System.out.println("\t'" + xqFile + "' Results");
        System.out.println("====================================================");

        if (opts.showQuery) {
            printField("Query", response.getStatement());
        }

        if (request.isShowMetrics()) {
            String metrics = String.format("Compile Time:\t%d\nElapsed Time:\t%d", response.getMetrics().getCompileTime(),
                    response.getMetrics().getElapsedTime());
            printField("Query Submission Metrics", metrics);
        }

        if (request.isShowAbstractSyntaxTree()) {
            printField("Abstract Syntax Tree", response.getAbstractSyntaxTree());
        }

        if (request.isShowTranslatedExpressionTree()) {
            printField("Translated Expression Tree", response.getTranslatedExpressionTree());
        }

        if (request.isShowOptimizedExpressionTree()) {
            printField("Optimized Expression Tree", response.getOptimizedExpressionTree());
        }

        if (request.isShowRuntimePlan()) {
            printField("Runtime Plan", response.getRuntimePlan());
        }

//        System.out.println(String.format("Reading results for '%s', result ID: %d", xqFile, response.getResultId()));
        QueryResultRequest resultRequest = new QueryResultRequest(response.getResultId(), response.getRequestId());
        resultRequest.setShowMetrics(opts.timing);
        sendQueryResultRequest(xqFile, resultRequest, this);
    }

    private void onQueryResultFetchSuccess(String xqFile, QueryResultRequest request, QueryResultResponse response) {
        if (request.isShowMetrics()) {
            String metrics = String.format("Elapsed Time:\t%d", response.getMetrics().getElapsedTime());
            printField("Result Reading Metrics", metrics);
        }

        printField("Results", response.getResults());
    }

    private void onQueryFailure(String xqFile, ErrorResponse response) {
        if (response == null) {
            System.err.println(String.format("Unable to execute query in %s", xqFile));
            return;
        }

        System.err.println();
        System.err.println("====================================================");
        System.err.println("\t'" + xqFile + "' Errors");
        System.err.println("====================================================");

        Error error = response.getError();
        String errorMsg = String.format("Code:\t %d\nMessage:\t %s", error.getCode(), error.getMessage());
        printField(System.err, String.format("Unable to execute query in '%s'", xqFile), errorMsg);
    }


    /**
     * Submits a query to be executed by the REST API. Will call {@link #onQueryFailure(String, ErrorResponse)} if any
     * error occurs when submitting the query. Else will call {@link #onQuerySubmitSuccess(String, QueryRequest,
     * QueryResponse)} with the {@link QueryResponse}
     *
     * @param xqFile  .xq file with the query to be executed
     * @param request {@link QueryRequest} instance to be submitted to REST API
     * @param cli     cli class instance
     */
    private static void sendQueryRequest(String xqFile, QueryRequest request, VXQuery cli) {
        URI uri = null;
        try {
            uri = RestUtils.buildQueryURI(request, cli.restIpAddress, cli.restPort);
        } catch (URISyntaxException e) {
            System.err.println(String.format("Unable to build URI to call REST API for query: %s", request.getStatement()));
            cli.onQueryFailure(xqFile, null);
        }

        CloseableHttpClient httpClient = HttpClients.custom()
                                                 .setConnectionTimeToLive(20, TimeUnit.SECONDS)
                                                 .build();

        try {
            HttpGet httpGet = new HttpGet(uri);
            httpGet.setHeader(HttpHeaders.ACCEPT, CONTENT_TYPE_JSON);

            try (CloseableHttpResponse httpResponse = httpClient.execute(httpGet)) {
                HttpEntity entity = httpResponse.getEntity();

                String response = RestUtils.readEntity(entity);
                if (httpResponse.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                    cli.onQuerySubmitSuccess(xqFile, request, RestUtils.mapEntity(response, QueryResponse.class, CONTENT_TYPE_JSON));
                } else {
                    cli.onQueryFailure(xqFile, RestUtils.mapEntity(response, ErrorResponse.class, CONTENT_TYPE_JSON));
                }
            } catch (IOException e) {
                System.err.println("Error occurred when reading entity: " + e.getMessage());
                cli.onQueryFailure(xqFile, null);
            } catch (JAXBException e) {
                System.err.println("Error occurred when mapping query response: " + e.getMessage());
                cli.onQueryFailure(xqFile, null);
            }
        } finally {
            HttpClientUtils.closeQuietly(httpClient);
        }
    }

    private static void sendQueryResultRequest(String xqFile, QueryResultRequest request, VXQuery cli) {
        URI uri = null;
        try {
            uri = RestUtils.buildQueryResultURI(request, cli.restIpAddress, cli.restPort);
        } catch (URISyntaxException e) {
            System.err.println(String.format("Unable to build URI to fetch results for query in %s", xqFile));
            cli.onQueryFailure(xqFile, null);
        }

        CloseableHttpClient httpClient = HttpClients.custom()
                                                 .setConnectionTimeToLive(20, TimeUnit.SECONDS)
                                                 .build();

        try {
            HttpGet httpGet = new HttpGet(uri);
            httpGet.setHeader(HttpHeaders.ACCEPT, CONTENT_TYPE_JSON);

            try (CloseableHttpResponse httpResponse = httpClient.execute(httpGet)) {
                HttpEntity entity = httpResponse.getEntity();

                String response = RestUtils.readEntity(entity);
                if (httpResponse.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                    cli.onQueryResultFetchSuccess(xqFile, request, RestUtils.mapEntity(response, QueryResultResponse.class, CONTENT_TYPE_JSON));
                } else {
                    cli.onQueryFailure(xqFile, RestUtils.mapEntity(response, ErrorResponse.class, CONTENT_TYPE_JSON));
                }
            } catch (IOException e) {
                System.err.println("Error occurred when reading entity: " + e.getMessage());
                cli.onQueryFailure(xqFile, null);
            } catch (JAXBException e) {
                System.err.println("Error occurred when mapping query result response: " + e.getMessage());
                cli.onQueryFailure(xqFile, null);
            }
        } finally {
            HttpClientUtils.closeQuietly(httpClient);
        }
    }


    private static QueryRequest createQueryRequest(CmdLineOptions opts, String query) {
        QueryRequest request = new QueryRequest(query);
        request.setCompileOnly(opts.compileOnly);
        request.setOptimization(opts.optimizationLevel);
        request.setFrameSize(opts.frameSize);
        request.setRepeatExecutions(opts.repeatExec);
        request.setShowMetrics(opts.timing);
        request.setShowAbstractSyntaxTree(opts.showAST);
        request.setShowTranslatedExpressionTree(opts.showTET);
        request.setShowOptimizedExpressionTree(opts.showOET);
        request.setShowRuntimePlan(opts.showRP);

        return request;
    }

    /**
     * Start local virtual cluster with cluster controller node and node controller nodes. IP address provided for node
     * controller is localhost. Unassigned ports 39000 and 39001 are used for client and cluster port respectively.
     * Creates a new Hyracks connection with the IP address and client ports.
     *
     * @throws Exception
     */
    public String startLocalHyracks() throws Exception {
        String localAddress = InetAddress.getLocalHost().getHostAddress();
        CCConfig ccConfig = new CCConfig();
        ccConfig.clientNetIpAddress = localAddress;
        ccConfig.clientNetPort = 39000;
        ccConfig.clusterNetIpAddress = localAddress;
        ccConfig.clusterNetPort = 39001;
        ccConfig.httpPort = 39002;
        ccConfig.profileDumpPeriod = 10000;
        ccConfig.appCCMainClass = VXQueryApplication.class.getName();
        ccConfig.appArgs = Arrays.asList("-restPort", String.valueOf(opts.restPort));
        clusterControllerService = new ClusterControllerService(ccConfig);
        clusterControllerService.start();

        NCConfig ncConfig = new NCConfig();
        ncConfig.ccHost = "localhost";
        ncConfig.ccPort = 39001;
        ncConfig.clusterNetIPAddress = localAddress;
        ncConfig.dataIPAddress = localAddress;
        ncConfig.resultIPAddress = localAddress;
        ncConfig.nodeId = "nc";
        ncConfig.ioDevices = Files.createTempDirectory(ncConfig.nodeId).toString();
        nodeControllerService = new NodeControllerService(ncConfig);
        nodeControllerService.start();

        return localAddress;
    }

    /**
     * Shuts down the virtual cluster, along with all nodes and node execution, network and queue managers.
     *
     * @throws Exception
     */
    public void stopLocalHyracks() throws Exception {
        nodeControllerService.stop();
        clusterControllerService.stop();
    }

    /**
     * Reads the contents of file given in query into a String. The file is always closed. For XML files UTF-8 encoding
     * is used.
     *
     * @param query The query with filename to be processed
     * @return UTF-8 formatted query string
     * @throws IOException
     */
    private static String slurp(String query) throws IOException {
        return FileUtils.readFileToString(new File(query), "UTF-8");
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

    private static void printField(PrintStream out, String field, String value) {
        out.println();
        field = field + ":";
        out.print(field);

        String[] lines = value.split("\n");
        for (int i = 0; i < lines.length; i++) {
            int margin = 4;
            if (i != 0) {
                margin += field.length();
            }
            System.out.print(String.format("%1$" + margin + "s%2$s\n", "", lines[i]));
        }
    }

    private static void printField(String field, String value) {
        printField(System.out, field, value);
    }

    /**
     * Helper class with fields and methods to handle all command line options
     */
    private static class CmdLineOptions {
        @Option(name = "-rest-ip-address", usage = "IP Address of the ClusterController.")
        private String restIpAddress = null;

        @Option(name = "-rest-port", usage = "Port of the ClusterController. (default: 1098)")
        private int restPort = 8085;

        @Option(name = "-compileonly", usage = "Compile the query and stop.")
        private boolean compileOnly;

        @Option(name = "-O", usage = "Optimization Level. (default: Full Optimization)")
        private int optimizationLevel = Integer.MAX_VALUE;

        @Option(name = "-frame-size", usage = "Frame size in bytes. (default: 65,536)")
        private int frameSize = 65536;

        @Option(name = "-repeatexec", usage = "Number of times to repeat execution.")
        private int repeatExec = 1;

        @Option(name = "-timing", usage = "Produce timing information.")
        private boolean timing;

        @Option(name = "-showquery", usage = "Show query string.")
        private boolean showQuery;

        @Option(name = "-showast", usage = "Show abstract syntax tree.")
        private boolean showAST;

        @Option(name = "-showtet", usage = "Show translated expression tree.")
        private boolean showTET;

        @Option(name = "-showoet", usage = "Show optimized expression tree.")
        private boolean showOET;

        @Option(name = "-showrp", usage = "Show Runtime plan.")
        private boolean showRP;

        // Optional (Not supported by REST API) parameters. Only used for creating a local hyracks cluster
        @Option(name = "-join-hash-size", usage = "Join hash size in bytes. (default: 67,108,864)")
        private long joinHashSize = -1;

        @Option(name = "-maximum-data-size", usage = "Maximum possible data size in bytes. (default: 150,323,855,000)")
        private long maximumDataSize = -1;

        @Option(name = "-buffer-size", usage = "Disk read buffer size in bytes.")
        private int bufferSize = -1;

        @Option(name = "-result-file", usage = "File path to save the query result.")
        private String resultFile = null;

        @Option(name = "-timing-ignore-queries", usage = "Ignore the first X number of quereies.")
        private int timingIgnoreQueries = 2;

        @Option(name = "-hdfs-conf", usage = "Directory path to Hadoop configuration files")
        private String hdfsConf = null;

        @Option(name = "-available-processors", usage = "Number of available processors. (default: java's available processors)")
        private int availableProcessors = -1;

        @Option(name = "-local-node-controllers", usage = "Number of local node controllers. (default: 1)")
        private int localNodeControllers = 1;

        @Argument
        private List<String> xqFiles = new ArrayList<>();
    }

}