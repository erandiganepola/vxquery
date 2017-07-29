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

package org.apache.vxquery.core;

import java.util.HashMap;
import java.util.Map;

/**
 * A class to store default/user specified configurations required at runtime by the {@link VXQuery} class. These
 * configuration will be loaded through a properties file.
 *
 * @author Erandi Ganepola
 */
public class VXQueryConfig {

    /** Number of available processors. (default: java's available processors) */
    private int availableProcessors = -1;
    /** Number of local node controllers. (default: 1) */
    private int localNodeControllers = 1;
    /** Join hash size in bytes. (default: 67,108,864) */
    private long joinHashSize = -1;
    /** Maximum possible data size in bytes. (default: 150,323,855,000) */
    private long maximumDataSize = -1;
    /** Bind an external variable */
    private Map<String, String> bindings = new HashMap<>();
    /** Directory path to Hadoop configuration files */
    private String hdfsConf = null;

    private String hyracksClientIp;
    private int hyracksClientPort;

    public int getAvailableProcessors() {
        return availableProcessors;
    }

    public void setAvailableProcessors(int availableProcessors) {
        this.availableProcessors = availableProcessors;
    }

    public int getLocalNodeControllers() {
        return localNodeControllers;
    }

    public void setLocalNodeControllers(int localNodeControllers) {
        this.localNodeControllers = localNodeControllers;
    }

    public long getJoinHashSize() {
        return joinHashSize;
    }

    public void setJoinHashSize(long joinHashSize) {
        this.joinHashSize = joinHashSize;
    }

    public long getMaximumDataSize() {
        return maximumDataSize;
    }

    public void setMaximumDataSize(long maximumDataSize) {
        this.maximumDataSize = maximumDataSize;
    }

    public Map<String, String> getBindings() {
        return bindings;
    }

    public void setBindings(Map<String, String> bindings) {
        this.bindings = bindings;
    }

    public String getHdfsConf() {
        return hdfsConf;
    }

    public void setHdfsConf(String hdfsConf) {
        this.hdfsConf = hdfsConf;
    }

    public int getHyracksClientPort() {
        return hyracksClientPort;
    }

    public void setHyracksClientPort(int hyracksClientPort) {
        this.hyracksClientPort = hyracksClientPort;
    }

    public String getHyracksClientIp() {
        return hyracksClientIp;
    }

    public void setHyracksClientIp(String hyracksClientIp) {
        this.hyracksClientIp = hyracksClientIp;
    }
}
