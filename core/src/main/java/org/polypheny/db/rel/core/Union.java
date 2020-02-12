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

package org.polypheny.db.rel.core;


import java.util.List;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.RelInput;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.metadata.RelMdUtil;
import org.polypheny.db.rel.metadata.RelMetadataQuery;
import org.polypheny.db.sql.SqlKind;


/**
 * Relational expression that returns the union of the rows of its inputs, optionally eliminating duplicates.
 *
 * Corresponds to SQL {@code UNION} and {@code UNION ALL}.
 */
public abstract class Union extends SetOp {

    protected Union( RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs, boolean all ) {
        super( cluster, traits, inputs, SqlKind.UNION, all );
    }


    /**
     * Creates a Union by parsing serialized output.
     */
    protected Union( RelInput input ) {
        super( input );
    }


    @Override
    public double estimateRowCount( RelMetadataQuery mq ) {
        double dRows = RelMdUtil.getUnionAllRowCount( mq, this );
        if ( !all ) {
            dRows *= 0.5;
        }
        return dRows;
    }

}

