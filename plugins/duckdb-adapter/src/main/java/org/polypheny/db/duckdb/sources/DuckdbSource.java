/*
 * Copyright 2019-2024 The Polypheny Project
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
 */

package org.polypheny.db.duckdb.sources;


import java.io.File;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbcp2.BasicDataSource;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.annotations.AdapterProperties;
import org.polypheny.db.adapter.annotations.AdapterSettingInteger;
import org.polypheny.db.adapter.annotations.AdapterSettingString;
import org.polypheny.db.adapter.annotations.AdapterSettingList;
import org.polypheny.db.adapter.jdbc.JdbcTable;
import org.polypheny.db.adapter.jdbc.connection.ConnectionFactory;
import org.polypheny.db.adapter.jdbc.connection.TransactionalConnectionFactory;
import org.polypheny.db.adapter.jdbc.sources.AbstractJdbcSource;
import org.polypheny.db.catalog.entity.allocation.AllocationTableWrapper;
import org.polypheny.db.catalog.entity.logical.LogicalTableWrapper;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.plugins.PolyPluginManager;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.sql.language.SqlDialect;
import org.polypheny.db.util.PolyphenyHomeDirManager;


@Slf4j
@AdapterProperties(
        name = "DuckDB",
        description = "DuckDB is an embedded OLAP RDBMS",
        usedModes = DeployMode.EMBEDDED,
        defaultMode = DeployMode.EMBEDDED)
@AdapterSettingString(name = "path", defaultValue = "", description = "The path to a DuckDB database file, in-memory database is currently not supported")
@AdapterSettingString(name = "tables", defaultValue = "", description = "List of tables which should be imported. The names must to be separated by a comma.")
@AdapterSettingString(name = "database", defaultValue = "")
public class DuckdbSource extends AbstractJdbcSource {

    String connectionUrl;

    public DuckdbSource( final long storeId, final String uniqueName, final Map<String, String> settings ) {
        super( storeId, uniqueName, settings, "org.duckdb.DuckDBDriver", PostgresqlSqlDialect.DEFAULT, false );
        this.connectionFactory = createConnectionFactory( settings, dialect, "org.duckdb.DuckDBDriver" );
        try { // need this to load duckdb driver
            Class.forName("org.duckdb.DuckDBDriver");
        } catch (Exception e) {
            System.out.println(e.toString());
        }
    }


    @Override
    protected ConnectionFactory createConnectionFactory( final Map<String, String> settings, SqlDialect dialect, String driverClass ) {
        BasicDataSource dataSource = new BasicDataSource();
        connectionUrl = "jdbc:duckdb:" + settings.get( "path" );
        dataSource.setUrl( connectionUrl );
        dataSource.setDriverClassName( "org.duckdb.DuckDBDriver" );
        dataSource.setDefaultAutoCommit( false );
        dataSource.setDriverClassLoader( PolyPluginManager.getMainClassLoader() );
        return new TransactionalConnectionFactory( dataSource, 25, dialect );
    }


    @Override
    public void shutdown() {
        try {
            removeInformationPage();
            connectionFactory.close();
        } catch ( SQLException e ) {
            log.warn( "Exception while shutting down {}", getUniqueName(), e );
        }
    }


    @Override
    protected void reloadSettings( List<String> updatedSettings ) {
        // TODO: Implement disconnect and reconnect to PostgreSQL instance.
    }


    @Override
    protected boolean requiresSchema() {
        return false;
    }

    @Override
    protected String getConnectionUrl( final String dbHostname, final int dbPort, final String dbName ) {
        return this.connectionUrl;
    }


    @Override
    public List<PhysicalEntity> createTable( Context context, LogicalTableWrapper logical, AllocationTableWrapper allocation ) {
        PhysicalTable table = adapterCatalog.createTable(
                logical.table.getNamespaceName(),
                logical.table.name,
                logical.columns.stream().collect( Collectors.toMap( c -> c.id, c -> c.name ) ),
                logical.table,
                logical.columns.stream().collect( Collectors.toMap( t -> t.id, t -> t ) ),
                logical.pkIds,
                allocation );

        JdbcTable physical = currentJdbcSchema.createJdbcTable( table );

        adapterCatalog.replacePhysical( physical );

        return List.of( physical );
    }

}
