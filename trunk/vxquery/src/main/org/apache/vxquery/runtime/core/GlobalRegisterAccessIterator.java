/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.vxquery.runtime.core;

import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.CallStackFrame;
import org.apache.vxquery.runtime.GlobalRegisterAccessor;
import org.apache.vxquery.runtime.LocalRegisterAccessor;
import org.apache.vxquery.runtime.RegisterAllocator;
import org.apache.vxquery.runtime.base.RuntimeIterator;

public class GlobalRegisterAccessIterator implements RuntimeIterator {
    private final GlobalRegisterAccessor<Object> regAccess;
    private final LocalRegisterAccessor<Boolean> done;

    public GlobalRegisterAccessIterator(RegisterAllocator rAllocator, int register) {
        regAccess = new GlobalRegisterAccessor<Object>(register);
        done = new LocalRegisterAccessor<Boolean>(rAllocator.allocate(1));
    }

    @Override
    public void open(CallStackFrame frame) {
        done.set(frame, false);
    }

    @Override
    public void close(CallStackFrame frame) {
    }

    @Override
    public Object next(CallStackFrame frame) throws SystemException {
        if (done.get(frame)) {
            return null;
        }
        done.set(frame, true);
        return regAccess.get(frame);
    }

    @Override
    public Object evaluateEagerly(CallStackFrame frame) throws SystemException {
        return regAccess.get(frame);
    }
}