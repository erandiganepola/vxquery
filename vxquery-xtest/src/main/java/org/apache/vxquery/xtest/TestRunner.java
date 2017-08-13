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
package org.apache.vxquery.xtest;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.HttpClientUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.dataset.IHyracksDataset;
import org.apache.vxquery.app.util.RestUtils;
import org.apache.vxquery.rest.request.QueryRequest;
import org.apache.vxquery.rest.response.SyncQueryResponse;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.vxquery.rest.Constants.HttpHeaderValues.CONTENT_TYPE_JSON;

public class TestRunner {
    private List<String> collectionList;
    private XTestOptions opts;
    private IHyracksClientConnection hcc;
    private IHyracksDataset hds;
    private static TestConfiguration indexConf;

    public TestRunner(XTestOptions opts) throws UnknownHostException {
        this.opts = opts;
        this.collectionList = new ArrayList<String>();
    }

    public void open() throws Exception {
        hcc = TestClusterUtil.getConnection();
        hds = TestClusterUtil.getDataset();
    }

    protected static TestConfiguration getIndexConfiguration(TestCase testCase) {
        XTestOptions opts = new XTestOptions();
        opts.verbose = false;
        opts.threads = 1;
        opts.showQuery = true;
        opts.showResult = true;
        opts.hdfsConf = "src/test/resources/hadoop/conf";
        opts.catalog = StringUtils.join(new String[] { "src", "test", "resources", "VXQueryCatalog.xml" },
                File.separator);
        TestConfiguration indexConf = new TestConfiguration();
        indexConf.options = opts;
        String baseDir = new File(opts.catalog).getParent();
        try {
            String root = new File(baseDir).getCanonicalPath();
            indexConf.testRoot = new File(root + "/./");
            indexConf.resultOffsetPath = new File(root + "/./ExpectedResults/");
            indexConf.sourceFileMap = testCase.getSourceFileMap();
            indexConf.xqueryFileExtension = ".xq";
            indexConf.xqueryxFileExtension = "xqx";
            indexConf.xqueryQueryOffsetPath = new File(root + "/./Queries/XQuery/");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return indexConf;
    }

    public TestCaseResult run(final TestCase testCase) {
        TestCaseResult res = new TestCaseResult(testCase);
        TestCase testCaseIndex = new TestCase(getIndexConfiguration(testCase));
        testCaseIndex.setFolder("Indexing/Partition-1/");
        testCaseIndex.setName("showIndexes");
        runQuery(testCaseIndex, res);
        String[] collections = res.result.split("\n");
        this.collectionList = Arrays.asList(collections);
        runQueries(testCase, res);
        return res;
    }

    public void runQuery(TestCase testCase, TestCaseResult res) {
        if (opts.verbose) {
            System.err.println("Starting " + testCase.getXQueryDisplayName());
        }

        long start = System.currentTimeMillis();

        try {
            String query = FileUtils.readFileToString(testCase.getXQueryFile(), "UTF-8");

            if (opts.showQuery) {
                System.err.println("***Query for " + testCase.getXQueryDisplayName() + ": ");
                System.err.println(query);
            }

            QueryRequest request = createQueryRequest(opts, query);
            SyncQueryResponse queryResponse = sendQueryRequest(request, opts);
            if (queryResponse == null) {
                System.err.println("Empty response: Error occurred when obtaining QueryResponse from REST API");
            }

            res.result = queryResponse.getResults();
        } catch (Throwable e) {
            res.error = e;
        } finally {
            try {
                res.compare();
            } catch (Exception e) {
                System.err.println("Framework error");
                e.printStackTrace();
            }
            long end = System.currentTimeMillis();
            res.time = end - start;
        }

        if (opts.showResult) {
            if (res.result == null) {
                System.err.println("***Error: ");
                System.err.println("Message: " + res.error.getMessage());
                res.error.printStackTrace();
            } else {
                System.err.println("***Result: ");
                System.err.println(res.result);
            }
        }
    }

    private static QueryRequest createQueryRequest(XTestOptions opts, String query) {
        QueryRequest request = new QueryRequest(query);
        request.setCompileOnly(opts.compileOnly);
        request.setOptimization(opts.optimizationLevel);
        request.setFrameSize(opts.frameSize);
//        request.setRepeatExecutions(opts.repeatExec);
        request.setShowAbstractSyntaxTree(opts.showAST);
        request.setShowTranslatedExpressionTree(opts.showTET);
        request.setShowOptimizedExpressionTree(opts.showOET);
        request.setShowRuntimePlan(opts.showRP);
        request.setAsync(false);

        return request;
    }

    private static SyncQueryResponse sendQueryRequest(QueryRequest request, XTestOptions opts) throws IOException, URISyntaxException {
        URI uri = RestUtils.buildQueryURI(request,
                TestClusterUtil.localClusterUtil.getIpAddress(),
                TestClusterUtil.localClusterUtil.getRestPort());
        CloseableHttpClient httpClient = HttpClients.custom().build();

        try {
            HttpGet httpGet = new HttpGet(uri);
            httpGet.setHeader(HttpHeaders.ACCEPT, CONTENT_TYPE_JSON);

            try (CloseableHttpResponse httpResponse = httpClient.execute(httpGet)) {
                HttpEntity entity = httpResponse.getEntity();
                if (httpResponse.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                    String response = RestUtils.readEntity(entity);
                    return RestUtils.mapEntity(response, SyncQueryResponse.class, CONTENT_TYPE_JSON);
                }
            } catch (IOException e) {
                System.err.println("Error occurred when reading entity: " + e.getMessage());
            } catch (JAXBException e) {
                System.err.println("Error occurred when mapping query response: " + e.getMessage());
            }
        } finally {
            HttpClientUtils.closeQuietly(httpClient);
        }

        return null;
    }

    public void runQueries(TestCase testCase, TestCaseResult res) {
        runQuery(testCase, res);
    }

    public void close() throws Exception {
        // TODO add a close statement for the hyracks connection.
    }
}
