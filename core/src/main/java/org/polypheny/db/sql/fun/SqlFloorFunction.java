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

package org.polypheny.db.sql.fun;


import com.google.common.base.Preconditions;
import org.polypheny.db.sql.SqlCall;
import org.polypheny.db.sql.SqlFunction;
import org.polypheny.db.sql.SqlFunctionCategory;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlLiteral;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlOperatorBinding;
import org.polypheny.db.sql.SqlUtil;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.sql.type.OperandTypes;
import org.polypheny.db.sql.type.ReturnTypes;
import org.polypheny.db.sql.validate.SqlMonotonicity;


/**
 * Definition of the "FLOOR" and "CEIL" built-in SQL functions.
 */
public class SqlFloorFunction extends SqlMonotonicUnaryFunction {


    public SqlFloorFunction( SqlKind kind ) {
        super( kind.name(),
                kind,
                ReturnTypes.ARG0_OR_EXACT_NO_SCALE,
                null,
                OperandTypes.or(
                        OperandTypes.NUMERIC_OR_INTERVAL,
                        OperandTypes.sequence(
                                "'" + kind + "(<DATE> TO <TIME_UNIT>)'\n"
                                        + "'" + kind + "(<TIME> TO <TIME_UNIT>)'\n"
                                        + "'" + kind + "(<TIMESTAMP> TO <TIME_UNIT>)'",
                                OperandTypes.DATETIME,
                                OperandTypes.ANY ) ),
                SqlFunctionCategory.NUMERIC );
        Preconditions.checkArgument( kind == SqlKind.FLOOR || kind == SqlKind.CEIL );
    }


    @Override
    public SqlMonotonicity getMonotonicity( SqlOperatorBinding call ) {
        // Monotonic iff its first argument is, but not strict.
        return call.getOperandMonotonicity( 0 ).unstrict();
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        final SqlWriter.Frame frame = writer.startFunCall( getName() );
        if ( call.operandCount() == 2 ) {
            call.operand( 0 ).unparse( writer, 0, 100 );
            writer.sep( "TO" );
            call.operand( 1 ).unparse( writer, 100, 0 );
        } else {
            call.operand( 0 ).unparse( writer, 0, 0 );
        }
        writer.endFunCall( frame );
    }


    /**
     * Copies a {@link SqlCall}, replacing the time unit operand with the given literal.
     *
     * @param call Call
     * @param literal Literal to replace time unit with
     * @param pos Parser position
     * @return Modified call
     */
    public static SqlCall replaceTimeUnitOperand( SqlCall call, String literal, SqlParserPos pos ) {
        SqlLiteral literalNode = SqlLiteral.createCharString( literal, null, pos );
        return call.getOperator().createCall( call.getFunctionQuantifier(), pos, call.getOperandList().get( 0 ), literalNode );
    }


    /**
     * Most dialects that natively support datetime floor will use this.
     * In those cases the call will look like TRUNC(datetime, 'year').
     *
     * @param writer SqlWriter
     * @param call SqlCall
     * @param funName Name of the sql function to call
     * @param datetimeFirst Specify the order of the datetime &amp; timeUnit arguments
     */
    public static void unparseDatetimeFunction( SqlWriter writer, SqlCall call, String funName, Boolean datetimeFirst ) {
        SqlFunction func = new SqlFunction(
                funName,
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.ARG0_NULLABLE_VARYING,
                null,
                null,
                SqlFunctionCategory.STRING );

        SqlCall call1;
        if ( datetimeFirst ) {
            call1 = call;
        } else {
            // switch order of operands
            SqlNode op1 = call.operand( 0 );
            SqlNode op2 = call.operand( 1 );

            call1 = call.getOperator().createCall( call.getParserPosition(), op2, op1 );
        }

        SqlUtil.unparseFunctionSyntax( func, writer, call1 );
    }
}
