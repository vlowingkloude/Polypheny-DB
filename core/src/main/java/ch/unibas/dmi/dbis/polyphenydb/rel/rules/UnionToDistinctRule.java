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

package ch.unibas.dmi.dbis.polyphenydb.rel.rules;


import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRule;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRuleCall;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.RelFactories;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Union;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalUnion;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilder;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilderFactory;


/**
 * Planner rule that translates a distinct {@link ch.unibas.dmi.dbis.polyphenydb.rel.core.Union} (<code>all</code> = <code>false</code>) into an {@link ch.unibas.dmi.dbis.polyphenydb.rel.core.Aggregate}
 * on top of a non-distinct {@link ch.unibas.dmi.dbis.polyphenydb.rel.core.Union} (<code>all</code> = <code>true</code>).
 */
public class UnionToDistinctRule extends RelOptRule {

    public static final UnionToDistinctRule INSTANCE = new UnionToDistinctRule( LogicalUnion.class, RelFactories.LOGICAL_BUILDER );


    /**
     * Creates a UnionToDistinctRule.
     */
    public UnionToDistinctRule( Class<? extends Union> unionClazz, RelBuilderFactory relBuilderFactory ) {
        super( operand( unionClazz, any() ), relBuilderFactory, null );
    }


    @Override
    public void onMatch( RelOptRuleCall call ) {
        final Union union = call.rel( 0 );
        if ( union.all ) {
            return; // nothing to do
        }
        final RelBuilder relBuilder = call.builder();
        relBuilder.pushAll( union.getInputs() );
        relBuilder.union( true, union.getInputs().size() );
        relBuilder.distinct();
        call.transformTo( relBuilder.build() );
    }
}

