/*
 * Copyright 2019-2020 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.unibas.dmi.dbis.polyphenydb.sql.validate;


import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCall;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;


/**
 * Namespace for COLLECT and TABLE constructs.
 *
 * Examples:
 *
 * <ul>
 * <li><code>SELECT deptno, COLLECT(empno) FROM emp GROUP BY deptno</code></li>,
 * <li><code>SELECT * FROM (TABLE getEmpsInDept(30))</code></li>.
 * </ul>
 *
 * NOTE: jhyde, 2006/4/24: These days, this class seems to be used exclusively for the <code>MULTISET</code> construct.
 *
 * @see CollectScope
 */
public class CollectNamespace extends AbstractNamespace {

    private final SqlCall child;
    private final SqlValidatorScope scope;


    /**
     * Creates a CollectNamespace.
     *
     * @param child Parse tree node
     * @param scope Scope
     * @param enclosingNode Enclosing parse tree node
     */
    CollectNamespace( SqlCall child, SqlValidatorScope scope, SqlNode enclosingNode ) {
        super( (SqlValidatorImpl) scope.getValidator(), enclosingNode );
        this.child = child;
        this.scope = scope;
        assert child.getKind() == SqlKind.MULTISET_VALUE_CONSTRUCTOR || child.getKind() == SqlKind.MULTISET_QUERY_CONSTRUCTOR;
    }


    @Override
    protected RelDataType validateImpl( RelDataType targetRowType ) {
        return child.getOperator().deriveType( validator, scope, child );
    }


    @Override
    public SqlNode getNode() {
        return child;
    }


    public SqlValidatorScope getScope() {
        return scope;
    }
}

