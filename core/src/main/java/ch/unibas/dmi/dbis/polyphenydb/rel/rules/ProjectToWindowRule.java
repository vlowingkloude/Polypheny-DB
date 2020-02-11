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


import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRule;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRuleCall;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRuleOperand;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Calc;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Project;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.RelFactories;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalCalc;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalWindow;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexCall;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexDynamicParam;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexFieldAccess;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexLiteral;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexLocalRef;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexOver;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexProgram;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexVisitorImpl;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexWindow;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilder;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilderFactory;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableIntList;
import ch.unibas.dmi.dbis.polyphenydb.util.Pair;
import ch.unibas.dmi.dbis.polyphenydb.util.graph.DefaultDirectedGraph;
import ch.unibas.dmi.dbis.polyphenydb.util.graph.DefaultEdge;
import ch.unibas.dmi.dbis.polyphenydb.util.graph.DirectedGraph;
import ch.unibas.dmi.dbis.polyphenydb.util.graph.TopologicalOrderIterator;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.linq4j.Ord;


/**
 * Planner rule that slices a {@link Project} into sections which contain windowed aggregate functions and sections which do not.
 *
 * The sections which contain windowed agg functions become instances of {@link LogicalWindow}. If the {@link LogicalCalc} does not contain
 * any windowed agg functions, does nothing.
 *
 * There is also a variant that matches {@link Calc} rather than {@code Project}.
 */
public abstract class ProjectToWindowRule extends RelOptRule {

    public static final ProjectToWindowRule INSTANCE = new CalcToWindowRule( RelFactories.LOGICAL_BUILDER );

    public static final ProjectToWindowRule PROJECT = new ProjectToLogicalProjectAndWindowRule( RelFactories.LOGICAL_BUILDER );


    /**
     * Creates a ProjectToWindowRule.
     *
     * @param operand Root operand, must not be null
     * @param description Description, or null to guess description
     * @param relBuilderFactory Builder for relational expressions
     */
    public ProjectToWindowRule( RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory, String description ) {
        super( operand, relBuilderFactory, description );
    }


    /**
     * Instance of the rule that applies to a {@link Calc} that contains windowed aggregates and converts it into a mixture
     * of {@link LogicalWindow} and {@code Calc}.
     */
    public static class CalcToWindowRule extends ProjectToWindowRule {

        /**
         * Creates a CalcToWindowRule.
         *
         * @param relBuilderFactory Builder for relational expressions
         */
        public CalcToWindowRule( RelBuilderFactory relBuilderFactory ) {
            super(
                    operandJ( Calc.class, null, calc -> RexOver.containsOver( calc.getProgram() ), any() ),
                    relBuilderFactory, "ProjectToWindowRule" );
        }


        @Override
        public void onMatch( RelOptRuleCall call ) {
            Calc calc = call.rel( 0 );
            assert RexOver.containsOver( calc.getProgram() );
            final CalcRelSplitter transform = new WindowedAggRelSplitter( calc, call.builder() );
            RelNode newRel = transform.execute();
            call.transformTo( newRel );
        }
    }


    /**
     * Instance of the rule that can be applied to a {@link Project} and that produces, in turn,
     * a mixture of {@code LogicalProject} and {@link LogicalWindow}.
     */
    public static class ProjectToLogicalProjectAndWindowRule extends ProjectToWindowRule {

        /**
         * Creates a ProjectToWindowRule.
         *
         * @param relBuilderFactory Builder for relational expressions
         */
        public ProjectToLogicalProjectAndWindowRule( RelBuilderFactory relBuilderFactory ) {
            super(
                    operandJ(
                            Project.class,
                            null,
                            project -> RexOver.containsOver( project.getProjects(), null ),
                            any() ),
                    relBuilderFactory,
                    "ProjectToWindowRule:project" );
        }


        @Override
        public void onMatch( RelOptRuleCall call ) {
            Project project = call.rel( 0 );
            assert RexOver.containsOver( project.getProjects(), null );
            final RelNode input = project.getInput();
            final RexProgram program =
                    RexProgram.create(
                            input.getRowType(),
                            project.getProjects(),
                            null,
                            project.getRowType(),
                            project.getCluster().getRexBuilder() );
            // temporary LogicalCalc, never registered
            final LogicalCalc calc = LogicalCalc.create( input, program );
            final CalcRelSplitter transform = new WindowedAggRelSplitter( calc, call.builder() ) {
                @Override
                protected RelNode handle( RelNode rel ) {
                    if ( !(rel instanceof LogicalCalc) ) {
                        return rel;
                    }
                    final LogicalCalc calc = (LogicalCalc) rel;
                    final RexProgram program = calc.getProgram();
                    relBuilder.push( calc.getInput() );
                    if ( program.getCondition() != null ) {
                        relBuilder.filter( program.expandLocalRef( program.getCondition() ) );
                    }
                    if ( !program.projectsOnlyIdentity() ) {
                        relBuilder.project(
                                Lists.transform( program.getProjectList(), program::expandLocalRef ),
                                calc.getRowType().getFieldNames() );
                    }
                    return relBuilder.build();
                }
            };
            RelNode newRel = transform.execute();
            call.transformTo( newRel );
        }
    }


    /**
     * Splitter that distinguishes between windowed aggregation expressions (calls to {@link RexOver}) and ordinary expressions.
     */
    static class WindowedAggRelSplitter extends CalcRelSplitter {

        private static final RelType[] REL_TYPES = {
                new RelType( "CalcRelType" ) {
                    @Override
                    protected boolean canImplement( RexFieldAccess field ) {
                        return true;
                    }


                    @Override
                    protected boolean canImplement( RexDynamicParam param ) {
                        return true;
                    }


                    @Override
                    protected boolean canImplement( RexLiteral literal ) {
                        return true;
                    }


                    @Override
                    protected boolean canImplement( RexCall call ) {
                        return !(call instanceof RexOver);
                    }


                    @Override
                    protected RelNode makeRel( RelOptCluster cluster, RelTraitSet traitSet, RelBuilder relBuilder, RelNode input, RexProgram program ) {
                        assert !program.containsAggs();
                        program = program.normalize( cluster.getRexBuilder(), null );
                        return super.makeRel( cluster, traitSet, relBuilder, input, program );
                    }
                },
                new RelType( "WinAggRelType" ) {
                    @Override
                    protected boolean canImplement( RexFieldAccess field ) {
                        return false;
                    }


                    @Override
                    protected boolean canImplement( RexDynamicParam param ) {
                        return false;
                    }


                    @Override
                    protected boolean canImplement( RexLiteral literal ) {
                        return false;
                    }


                    @Override
                    protected boolean canImplement( RexCall call ) {
                        return call instanceof RexOver;
                    }


                    @Override
                    protected boolean supportsCondition() {
                        return false;
                    }


                    @Override
                    protected RelNode makeRel( RelOptCluster cluster, RelTraitSet traitSet, RelBuilder relBuilder, RelNode input, RexProgram program ) {
                        Preconditions.checkArgument( program.getCondition() == null, "WindowedAggregateRel cannot accept a condition" );
                        return LogicalWindow.create( cluster, traitSet, relBuilder, input, program );
                    }
                }
        };


        WindowedAggRelSplitter( Calc calc, RelBuilder relBuilder ) {
            super( calc, relBuilder, REL_TYPES );
        }


        @Override
        protected List<Set<Integer>> getCohorts() {
            // Two RexOver will be put in the same cohort if the following conditions are satisfied
            // (1). They have the same RexWindow
            // (2). They are not dependent on each other
            final List<RexNode> exprs = this.program.getExprList();
            final DirectedGraph<Integer, DefaultEdge> graph = createGraphFromExpression( exprs );
            final List<Integer> rank = getRank( graph );

            final List<Pair<RexWindow, Set<Integer>>> windowToIndices = new ArrayList<>();
            for ( int i = 0; i < exprs.size(); ++i ) {
                final RexNode expr = exprs.get( i );
                if ( expr instanceof RexOver ) {
                    final RexOver over = (RexOver) expr;

                    // If we can found an existing cohort which satisfies the two conditions, we will add this RexOver into that cohort
                    boolean isFound = false;
                    for ( Pair<RexWindow, Set<Integer>> pair : windowToIndices ) {
                        // Check the first condition
                        if ( pair.left.equals( over.getWindow() ) ) {
                            // Check the second condition
                            boolean hasDependency = false;
                            for ( int ordinal : pair.right ) {
                                if ( isDependent( graph, rank, ordinal, i ) ) {
                                    hasDependency = true;
                                    break;
                                }
                            }

                            if ( !hasDependency ) {
                                pair.right.add( i );
                                isFound = true;
                                break;
                            }
                        }
                    }

                    // This RexOver cannot be added into any existing cohort
                    if ( !isFound ) {
                        final Set<Integer> newSet = new HashSet<>( ImmutableList.of( i ) );
                        windowToIndices.add( Pair.of( over.getWindow(), newSet ) );
                    }
                }
            }

            final List<Set<Integer>> cohorts = new ArrayList<>();
            for ( Pair<RexWindow, Set<Integer>> pair : windowToIndices ) {
                cohorts.add( pair.right );
            }
            return cohorts;
        }


        private boolean isDependent( final DirectedGraph<Integer, DefaultEdge> graph, final List<Integer> rank, final int ordinal1, final int ordinal2 ) {
            if ( rank.get( ordinal2 ) > rank.get( ordinal1 ) ) {
                return isDependent( graph, rank, ordinal2, ordinal1 );
            }

            // Check if the expression in ordinal1 could depend on expression in ordinal2 by Depth-First-Search
            final Deque<Integer> dfs = new ArrayDeque<>();
            final Set<Integer> visited = new HashSet<>();
            dfs.push( ordinal2 );
            while ( !dfs.isEmpty() ) {
                int source = dfs.pop();
                if ( visited.contains( source ) ) {
                    continue;
                }

                if ( source == ordinal1 ) {
                    return true;
                }

                visited.add( source );
                for ( DefaultEdge e : graph.getOutwardEdges( source ) ) {
                    int target = (int) e.target;
                    if ( rank.get( target ) < rank.get( ordinal1 ) ) {
                        dfs.push( target );
                    }
                }
            }

            return false;
        }


        private List<Integer> getRank( DirectedGraph<Integer, DefaultEdge> graph ) {
            final int[] rankArr = new int[graph.vertexSet().size()];
            int rank = 0;
            for ( int i : TopologicalOrderIterator.of( graph ) ) {
                rankArr[i] = rank++;
            }
            return ImmutableIntList.of( rankArr );
        }


        private DirectedGraph<Integer, DefaultEdge> createGraphFromExpression( final List<RexNode> exprs ) {
            final DirectedGraph<Integer, DefaultEdge> graph = DefaultDirectedGraph.create();
            for ( int i = 0; i < exprs.size(); i++ ) {
                graph.addVertex( i );
            }

            for ( final Ord<RexNode> expr : Ord.zip( exprs ) ) {
                expr.e.accept(
                        new RexVisitorImpl<Void>( true ) {
                            @Override
                            public Void visitLocalRef( RexLocalRef localRef ) {
                                graph.addEdge( localRef.getIndex(), expr.i );
                                return null;
                            }
                        } );
            }
            assert graph.vertexSet().size() == exprs.size();
            return graph;
        }
    }
}
