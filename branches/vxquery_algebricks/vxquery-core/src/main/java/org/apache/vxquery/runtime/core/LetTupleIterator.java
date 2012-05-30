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
import org.apache.vxquery.runtime.LocalRegisterAccessor;
import org.apache.vxquery.runtime.RegisterAllocator;
import org.apache.vxquery.runtime.RuntimeUtils;
import org.apache.vxquery.runtime.base.AbstractUnaryTupleIterator;
import org.apache.vxquery.runtime.base.RuntimeIterator;
import org.apache.vxquery.runtime.base.TupleIterator;
import org.apache.vxquery.v0datamodel.EagerlyMaterializedXDMSequenceImpl;

public class LetTupleIterator extends AbstractUnaryTupleIterator {
    private final RuntimeIterator sequence;
    private final LocalRegisterAccessor<Object> letVar;

    public LetTupleIterator(RegisterAllocator rAllocator, TupleIterator input, RuntimeIterator sequence, int letVarReg) {
        super(rAllocator, input);
        this.sequence = sequence;
        this.letVar = new LocalRegisterAccessor<Object>(letVarReg);
    }

    @Override
    public boolean next(CallStackFrame frame) throws SystemException {
        if (!input.next(frame)) {
            return false;
        }
        sequence.open(frame);
        letVar.set(frame, EagerlyMaterializedXDMSequenceImpl.inflate(RuntimeUtils.createCloseableIterator(frame,
                sequence)));
        return true;
    }
}