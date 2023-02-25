/*
 * Copyright 2019-2023 The Polypheny Project
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

package connect;

import lombok.extern.slf4j.Slf4j;
import org.polypheny.jdbc.Driver;
import java.sql.Connection;
import org.polypheny.jdbc.PolyphenyJdbcStatement;

import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;

@Slf4j
public class QueryLogAdapter {
    private static final String ADAPTER_UNIQUE_NAME = "query_log";
    private static final String ADAPTER = "postgresql";
    private static final String ADAPTER_CONFIG = """
            '{"mode":"docker","password":"polypheny","instanceId":"0","port":"5432","maxConnections":"25"}'""";
    private static final String SCHEMA_NAME = "polyfier";
    public QueryLogAdapter() {}

    public Optional<Connection> connect( String url, String user, String password ) {
        Properties props = new Properties();
        props.setProperty( "user", user );
        props.setProperty( "password", password );
        try {
            Driver driver = new Driver();
            Connection connection = (Connection) driver.connect( url, props );

            // Connection connection = DriverManager.getConnection( url, props );
            configure( connection );
            return Optional.of( connection );
        } catch ( SQLException e ) {
            throw new RuntimeException(e);
        }
    }

    public boolean isConfigured() {

        return false;
    }

    public boolean resetParameter() {
        // Todo implement reset
        return true;
    }

    public void configure( Connection connection ) throws SQLException {

        if ( ! isConfigured() || resetParameter() ) {
            PolyphenyJdbcStatement statement = (PolyphenyJdbcStatement) connection.createStatement();

            if ( resetParameter() ) {
                log.info("Reset Parameter is set...");
                dropTables( statement );
                dropSchema( statement );
                dropAdapter( statement );
            } else {
                log.info("Backend is not configured...");
            }

            log.info("Configuring Backend...");
            // Todo Implement Prepared Statements
            configureAdapter( statement );
            connection.commit();
            configureSchema( statement );
            configureTables( statement );
            connection.commit();
        }
    }

    private void configureAdapter( PolyphenyJdbcStatement statement ) {
        log.debug("Adding Adapters..");
        String polySql = """
                ALTER ADAPTERS ADD %s USING %s AS store WITH %s
        """;
        try {
            statement.execute( polySql.formatted( ADAPTER_UNIQUE_NAME, ADAPTER, ADAPTER_CONFIG ) );
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }



    private void configureSchema( PolyphenyJdbcStatement statement ) {
        log.debug("Creating Schema...");
        String polySql = "CREATE SCHEMA %s";
        try {
            statement.execute( polySql.formatted( SCHEMA_NAME ) );
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void configureTables( PolyphenyJdbcStatement statement ) {
        log.debug("Creating Tables...");
        try {
            // Create tasks Table
            String polySqlPolyfierTasksTable = new StringBuilder()
                    .append("CREATE TABLE ").append( SCHEMA_NAME ).append(".tasks " ).append("(\n")

                    // Fields
                    .append("\t").append("task_id ").append("bigint ").append("NOT NULL").append(",\n")
                    .append("\t").append("query_config ").append("file ").append(",\n")
                    .append("\t").append("data_config ").append("file ").append(",\n")

                    // Schema Type -> One of a number of predefined Schemas for now.
                    .append("\t").append("schema_type ").append("varchar ").append(",\n")

                    // Constraints
                    .append("\t").append("PRIMARY KEY ( ").append("task_id").append(" )\n")

                    // Store
                    .append(") ").append("ON STORE ").append( ADAPTER_UNIQUE_NAME )
                    .toString();

            log.debug("Create Table Statement: \n\n" + polySqlPolyfierTasksTable );
            statement.execute( polySqlPolyfierTasksTable );

            // Create configurations Table
            String polySqlPolyfierConfigurationTable = new StringBuilder()
                    .append("CREATE TABLE ").append( SCHEMA_NAME ).append(".configurations " ).append("(\n")

                    // Fields
                    .append("\t").append("config_id ").append("bigint ").append("NOT NULL").append(",\n")
                    // Config Files
                    .append("\t").append("store_config ").append("file ").append("NOT NULL").append(",\n")
                    .append("\t").append("partition_config ").append("file ").append("NOT NULL").append(",\n")
                    .append("\t").append("start_config ").append("file ").append("NOT NULL").append(",\n")
                    // File Hashes
                    .append("\t").append("store_config_hash ").append("bigint ").append("NOT NULL").append(",\n")
                    .append("\t").append("partition_config_hash ").append("bigint ").append("NOT NULL").append(",\n")
                    .append("\t").append("start_config_hash ").append("bigint ").append("NOT NULL").append(",\n")

                    // Constraints
                    .append("\t").append("PRIMARY KEY ( ").append("config_id").append(" )\n")

                    // Store
                    .append(") ").append("ON STORE ").append( ADAPTER_UNIQUE_NAME )
                    .toString();

            // Create clients Table
            String polySqlPolyfierClientsTable = new StringBuilder()
                    .append("CREATE TABLE ").append( SCHEMA_NAME ).append(".clients " ).append("(\n")

                    // Fields
                    .append("\t").append("client_id ").append("bigint ").append("NOT NULL").append(",\n")

                    // Client Information
                    .append("\t").append("branch ").append("varchar ").append("NOT NULL").append(",\n")
                    .append("\t").append("version ").append("varchar ").append("NOT NULL").append(",\n")
                    .append("\t").append("registered_at ").append("timestamp ").append("NOT NULL").append(",\n")

                    // Constraints
                    .append("\t").append("PRIMARY KEY ( ").append("client_id").append(" )\n")

                    // Store
                    .append(") ").append("ON STORE ").append( ADAPTER_UNIQUE_NAME )
                    .toString();

            log.debug("Create Table Statement: \n\n" + polySqlPolyfierClientsTable );
            statement.execute( polySqlPolyfierClientsTable );

            // Create execution_tasks Table
            String polySqlPolyfierExecTasksTable = new StringBuilder()
                    .append("CREATE TABLE ").append( SCHEMA_NAME ).append(".execution_tasks " ).append("(\n")

                    // Fields
                    .append("\t").append("exec_id ").append("bigint ").append("NOT NULL").append(",\n")
                    .append("\t").append("task_id ").append("bigint ").append("NOT NULL").append(",\n")
                    .append("\t").append("config_id ").append("bigint ").append("NOT NULL").append(",\n")
                    .append("\t").append("client_id ").append("bigint ").append("NOT NULL").append(",\n")
                    // Seed Range
                    .append("\t").append("seed_from ").append("bigint ").append("NOT NULL").append(",\n")
                    .append("\t").append("seed_to ").append("bigint ").append("NOT NULL").append(",\n")
                    // Timestamps
                    .append("\t").append("issued_at ").append("timestamp ").append("NOT NULL").append(",\n")
                    .append("\t").append("completed_at ").append("timestamp ").append("NULL " ).append("DEFAULT NULL").append(",\n")

                    // Constraints
                    .append("\t").append("PRIMARY KEY ( ").append("exec_id").append(" )\n")

                    // Store
                    .append(") ").append("ON STORE ").append( ADAPTER_UNIQUE_NAME )
                    .toString();


            log.debug("Create Table Statement: \n\n" + polySqlPolyfierExecTasksTable );
            statement.execute( polySqlPolyfierExecTasksTable );

            // Create polyfier_results Table
            String polySqlPolyfierResultsTable = new StringBuilder()
                    .append("CREATE TABLE ").append( SCHEMA_NAME ).append(".polyfier_results " ).append("(\n")

                    // Fields
                    .append("\t").append("index ").append("bigint ").append("NOT NULL").append(",\n")
                    // Associated Execution Order
                    .append("\t").append("exec_id ").append("bigint ").append("NOT NULL").append(",\n")
                    // Seed of generated query
                    .append("\t").append("seed ").append("bigint ").append("NOT NULL").append(",\n")
                    // If query completed without issues. If false -> Error not null.
                    .append("\t").append("success ").append("boolean ").append("NOT NULL").append(",\n")
                    // Time of Logging
                    .append("\t").append("received_at ").append("timestamp ").append("NOT NULL").append(",\n")
                    // Error in Case of Failure
                    .append("\t").append("error ").append("file ").append("NULL ").append("DEFAULT NULL").append(",\n")
                    // ResultSet
                    .append("\t").append("result_set ").append("file ").append("NULL").append(",\n")
                    // ResultSetHash
                    .append("\t").append("result_set_hash ").append("bigint ").append("NULL").append(",\n")
                    // Plans
                    .append("\t").append("logical ").append("file ").append("NULL").append(",\n")
                    .append("\t").append("physical ").append("file ").append("NULL").append(",\n")
                    // Plan Hashes
                    .append("\t").append("logical_hash ").append("bigint ").append("NULL").append(",\n")
                    .append("\t").append("physical_hash ").append("bigint ").append("NULL").append(",\n")
                    // Execution Time
                    .append("\t").append("actual ").append("bigint ").append("NULL").append(",\n")
                    .append("\t").append("predicted ").append("bigint ").append("NULL").append(",\n")

                    // Constraints
                    .append("\t").append("PRIMARY KEY ( ").append("index").append(" )\n")

                    // Store
                    .append(") ").append("ON STORE ").append( ADAPTER_UNIQUE_NAME )
                    .toString();

            log.debug("Create Table: \n\n" + polySqlPolyfierResultsTable );
            statement.execute( polySqlPolyfierResultsTable );


        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void dropTables( PolyphenyJdbcStatement statement ) {
        log.debug("Dropping Tables...");
        String polySql = """
                DROP TABLE IF EXISTS %s.%s
        """;
        try {
            statement.execute( polySql.formatted( SCHEMA_NAME, "polyfier_results" ) );
            statement.execute( polySql.formatted( SCHEMA_NAME, "polyfier_tasks" ) );
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    private void dropSchema( PolyphenyJdbcStatement statement ) {
        log.debug("Dropping Schema...");
        String polySql = """
                DROP SCHEMA IF EXISTS %s
        """;
        try {
            statement.execute( polySql.formatted( SCHEMA_NAME ) );
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    private void dropAdapter( PolyphenyJdbcStatement statement ) {
        log.debug("Dropping Adapters...");
        String polySql = """
                ALTER ADAPTERS DROP %s
        """;
        try {
            statement.execute( polySql.formatted( ADAPTER_UNIQUE_NAME ) );
        } catch ( SQLException e) {
            log.warn("Assuming Adapter does not exist. SQLException: ", e );
        }
    }


}
