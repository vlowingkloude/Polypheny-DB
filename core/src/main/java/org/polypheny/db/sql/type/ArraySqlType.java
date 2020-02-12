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

package org.polypheny.db.sql.type;


import java.util.Objects;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFamily;
import org.polypheny.db.rel.type.RelDataTypePrecedenceList;


/**
 * SQL array type.
 */
public class ArraySqlType extends AbstractSqlType {

    private final RelDataType elementType;


    /**
     * Creates an ArraySqlType. This constructor should only be called from a factory method.
     */
    public ArraySqlType( RelDataType elementType, boolean isNullable ) {
        super( SqlTypeName.ARRAY, isNullable, null );
        this.elementType = Objects.requireNonNull( elementType );
        computeDigest();
    }


    // implement RelDataTypeImpl
    @Override
    protected void generateTypeString( StringBuilder sb, boolean withDetail ) {
        if ( withDetail ) {
            sb.append( elementType.getFullTypeString() );
        } else {
            sb.append( elementType.toString() );
        }
        sb.append( " ARRAY" );
    }


    // implement RelDataType
    @Override
    public RelDataType getComponentType() {
        return elementType;
    }


    // implement RelDataType
    @Override
    public RelDataTypeFamily getFamily() {
        return this;
    }


    @Override
    public RelDataTypePrecedenceList getPrecedenceList() {
        return new RelDataTypePrecedenceList() {
            @Override
            public boolean containsType( RelDataType type ) {
                return type.getSqlTypeName() == getSqlTypeName()
                        && type.getComponentType() != null
                        && getComponentType().getPrecedenceList().containsType( type.getComponentType() );
            }


            @Override
            public int compareTypePrecedence( RelDataType type1, RelDataType type2 ) {
                if ( !containsType( type1 ) ) {
                    throw new IllegalArgumentException( "must contain type: " + type1 );
                }
                if ( !containsType( type2 ) ) {
                    throw new IllegalArgumentException( "must contain type: " + type2 );
                }
                return getComponentType().getPrecedenceList().compareTypePrecedence( type1.getComponentType(), type2.getComponentType() );
            }
        };
    }
}

