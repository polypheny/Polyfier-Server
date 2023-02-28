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

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;
import java.util.Properties;

@Slf4j
public class QueryLogAdapter {
    public static final String DOCUMENT_ADAPTER_UNIQUE_NAME = "doc_query_log";
    public static final String DOCUMENT_ADAPTER = "mongodb";
    public static final String DOCUMENT_ADAPTER_CONFIG = """
            '{"mode":"docker","instanceId":"0","port":"27017","trxLifetimeLimit":"1209600","persistent":"false"}'""";
    public static final String RELATIONAL_ADAPTER_UNIQUE_NAME = "rel_query_log";
    public static final String RELATIONAL_ADAPTER = "postgresql";
    public static final String RELATIONAL_ADAPTER_CONFIG = """
            '{"mode":"docker","password":"polypheny","instanceId":"0","port":"5436","maxConnections":"25"}'""";
    public static final String SCHEMA_NAME = "polyfier";
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
            Statement statement = (Statement) connection.createStatement();

            if ( resetParameter() ) {
                log.info("Reset Parameter is set...");
                dropTables( statement );
                dropSchema( statement );
                //dropAdapter( statement );
            } else {
                log.info("Backend is not configured...");
            }

            log.info("Configuring Backend...");
            // Todo Implement Prepared Statements
            configureAdapter( statement );
            configureSchema( statement );
            configureTables( statement );
            connection.commit();
        }
    }

    private void configureAdapter( Statement statement ) {
        log.debug("Adding Adapters..");
        String polySql = """
                ALTER ADAPTERS ADD %s USING %s AS store WITH %s
        """;
        try {
            statement.execute( polySql.formatted(
                    RELATIONAL_ADAPTER_UNIQUE_NAME,
                    RELATIONAL_ADAPTER,
                    RELATIONAL_ADAPTER_CONFIG
            ) );
            statement.execute( polySql.formatted(
                    DOCUMENT_ADAPTER_UNIQUE_NAME,
                    DOCUMENT_ADAPTER,
                    DOCUMENT_ADAPTER_CONFIG
            ) );
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }



    private void configureSchema( Statement statement ) {
        log.debug("Creating Schema...");
        String polySql = "CREATE SCHEMA polyfier";
        try {
            statement.execute( polySql );
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void configureTables( Statement statement ) {

        log.debug("Creating Tables...");

        try {

            String queryConfigurationDocument = new StringBuilder()
                .append("CREATE TABLE ").append( "polyfier.query_configurations " ).append("(\n")

                // Fields
                .append("\t").append("query_config_hash ").append("bigint ").append("NOT NULL ").append(",\n")
                // Config Files
                .append("\t").append("query_config ").append("varchar ").append("NOT NULL").append(",\n")
                // Constraints
                .append("\t").append("PRIMARY KEY ( ").append("query_config_hash").append(" )\n")

                // Store
                .append(") ").append("ON STORE ").append( DOCUMENT_ADAPTER_UNIQUE_NAME )
                .toString();

            log.debug("Create Table Statement: \n\n" + queryConfigurationDocument );
            statement.execute( queryConfigurationDocument );

            String dataConfigurationsDocument = new StringBuilder()
                    .append("CREATE TABLE ").append( "polyfier.data_configurations " ).append("(\n")

                    // Fields
                    .append("\t").append("data_config_hash ").append("bigint ").append("NOT NULL ").append(",\n")
                    // Config Files
                    .append("\t").append("data_config ").append("varchar ").append("NOT NULL").append(",\n")
                    // Constraints
                    .append("\t").append("PRIMARY KEY ( ").append("data_config_hash").append(" )\n")

                    // Store
                    .append(") ").append("ON STORE ").append( DOCUMENT_ADAPTER_UNIQUE_NAME )
                    .toString();

            log.debug("Create Table Statement: \n\n" + dataConfigurationsDocument );
            statement.execute( dataConfigurationsDocument );

            String errorsDocument = new StringBuilder()
                    .append("CREATE TABLE ").append( "polyfier.errors " ).append("(\n")

                    // Fields
                    .append("\t").append("error_hash ").append("bigint ").append("NOT NULL ").append(",\n")
                    // Config Files
                    .append("\t").append("error ").append("varchar ").append("NOT NULL").append(",\n")
                    // Constraints
                    .append("\t").append("PRIMARY KEY ( ").append("error_hash").append(" )\n")

                    // Store
                    .append(") ").append("ON STORE ").append( DOCUMENT_ADAPTER_UNIQUE_NAME )
                    .toString();

            log.debug("Create Table Statement: \n\n" + errorsDocument );
            statement.execute( errorsDocument );

            String physicalPlansDocument = new StringBuilder()
                    .append("CREATE TABLE ").append( "polyfier.physical_plans " ).append("(\n")

                    // Fields
                    .append("\t").append("physical_hash ").append("bigint ").append("NOT NULL ").append(",\n")
                    // Config Files
                    .append("\t").append("physical ").append("varchar ").append("NOT NULL").append(",\n")
                    // Constraints
                    .append("\t").append("PRIMARY KEY ( ").append("physical_hash").append(" )\n")

                    // Store
                    .append(") ").append("ON STORE ").append( DOCUMENT_ADAPTER_UNIQUE_NAME )
                    .toString();

            log.debug("Create Table Statement: \n\n" + physicalPlansDocument );
            statement.execute( physicalPlansDocument );

            String logicalPlansDocument = new StringBuilder()
                    .append("CREATE TABLE ").append( "polyfier.logical_plans " ).append("(\n")

                    // Fields
                    .append("\t").append("logical_hash ").append("bigint ").append("NOT NULL ").append(",\n")
                    // Config Files
                    .append("\t").append("logical ").append("varchar ").append("NOT NULL").append(",\n")
                    // Constraints
                    .append("\t").append("PRIMARY KEY ( ").append("logical_hash").append(" )\n")

                    // Store
                    .append(") ").append("ON STORE ").append( DOCUMENT_ADAPTER_UNIQUE_NAME )
                    .toString();

            log.debug("Create Table Statement: \n\n" + logicalPlansDocument );
            statement.execute( logicalPlansDocument );

            String storeConfigurationDocument = new StringBuilder()
                    .append("CREATE TABLE ").append( "polyfier.store_configurations " ).append("(\n")

                    // Fields
                    .append("\t").append("store_config_hash ").append("bigint ").append("NOT NULL ").append(",\n")
                    // Config Files
                    .append("\t").append("store_config ").append("varchar ").append("NOT NULL").append(",\n")
                    // Constraints
                    .append("\t").append("PRIMARY KEY ( ").append("store_config_hash").append(" )\n")

                    // Store
                    .append(") ").append("ON STORE ").append( DOCUMENT_ADAPTER_UNIQUE_NAME )
                    .toString();

            log.debug("Create Table Statement: \n\n" + storeConfigurationDocument );
            statement.execute( storeConfigurationDocument );

            String partitionConfigurationDocument = new StringBuilder()
                    .append("CREATE TABLE ").append( "polyfier.partition_configurations " ).append("(\n")

                    // Fields
                    .append("\t").append("partition_config_hash ").append("bigint ").append("NOT NULL ").append(",\n")
                    // Config Files
                    .append("\t").append("partition_config ").append("varchar ").append("NOT NULL").append(",\n")
                    // Constraints
                    .append("\t").append("PRIMARY KEY ( ").append("partition_config_hash").append(" )\n")

                    // Store
                    .append(") ").append("ON STORE ").append( DOCUMENT_ADAPTER_UNIQUE_NAME )
                    .toString();

            log.debug("Create Table Statement: \n\n" + partitionConfigurationDocument );
            statement.execute( partitionConfigurationDocument );

            String startConfigurationDocument = new StringBuilder()
                    .append("CREATE TABLE ").append( "polyfier.start_configurations " ).append("(\n")

                    // Fields
                    .append("\t").append("start_config_hash ").append("bigint ").append("NOT NULL ").append(",\n")
                    // Config Files
                    .append("\t").append("start_config ").append("varchar ").append("NOT NULL").append(",\n")
                    // Constraints
                    .append("\t").append("PRIMARY KEY ( ").append("start_config_hash").append(" )\n")

                    // Store
                    .append(") ").append("ON STORE ").append( DOCUMENT_ADAPTER_UNIQUE_NAME )
                    .toString();

            log.debug("Create Table Statement: \n\n" + startConfigurationDocument );
            statement.execute( startConfigurationDocument );

            // Create tasks Table
            String polySqlPolyfierTasksTable = new StringBuilder()
                    .append("CREATE TABLE ").append( "polyfier.tasks " ).append("(\n")

                    // Fields
                    .append("\t").append("task_id ").append("varchar(36) ").append("NOT NULL ").append(",\n")

                    // Config Files
                    .append("\t").append("query_config ").append("varchar ").append("NOT NULL").append(",\n")
                    .append("\t").append("data_config ").append("varchar ").append("NOT NULL").append(",\n")

                    // File Hashes
                    .append("\t").append("query_config_hash ").append("bigint ").append("NOT NULL").append(",\n")
                    .append("\t").append("data_config_hash ").append("bigint ").append("NOT NULL").append(",\n")

                    // Schema Type -> One of a number of predefined Schemas for now.
                    .append("\t").append("schema_type ").append("varchar ").append("NOT NULL").append(",\n")

                    // Constraints
                    .append("\t").append("PRIMARY KEY ( ").append("task_id").append(" )\n")

                    // Store
                    .append(") ").append("ON STORE ").append( RELATIONAL_ADAPTER_UNIQUE_NAME )
                    .toString();

            log.debug("Create Table Statement: \n\n" + polySqlPolyfierTasksTable );
            statement.execute( polySqlPolyfierTasksTable );

            // Create configurations Table
            String polySqlPolyfierConfigurationTable = new StringBuilder()
                    .append("CREATE TABLE ").append( "polyfier.configurations " ).append("(\n")

                    // Fields
                    .append("\t").append("config_id ").append("varchar(36) ").append("NOT NULL ").append(",\n")

                    // Config Files
                    .append("\t").append("store_config ").append("varchar ").append("NOT NULL").append(",\n")
                    .append("\t").append("partition_config ").append("varchar ").append("NOT NULL").append(",\n")
                    .append("\t").append("start_config ").append("varchar ").append("NOT NULL").append(",\n")

                    // File Hashes
                    .append("\t").append("store_config_hash ").append("bigint ").append("NOT NULL").append(",\n")
                    .append("\t").append("partition_config_hash ").append("bigint ").append("NOT NULL").append(",\n")
                    .append("\t").append("start_config_hash ").append("bigint ").append("NOT NULL").append(",\n")

                    // Constraints
                    .append("\t").append("PRIMARY KEY ( ").append("config_id").append(" )\n")

                    // Store
                    .append(") ").append("ON STORE ").append( RELATIONAL_ADAPTER_UNIQUE_NAME )
                    .toString();

            log.debug("Create Table Statement: \n\n" + polySqlPolyfierConfigurationTable );
            statement.execute( polySqlPolyfierConfigurationTable );


            // Create clients Table
            String polySqlPolyfierClientsTable = new StringBuilder()
                    .append("CREATE TABLE ").append( "polyfier.clients " ).append("(\n")

                    // Fields
                    .append("\t").append("client_id ").append("varchar(36) ").append("NOT NULL ").append(",\n")

                    // Client Information
                    .append("\t").append("branch ").append("varchar ").append("NOT NULL").append(",\n")
                    .append("\t").append("version ").append("varchar ").append("NOT NULL").append(",\n")
                    .append("\t").append("registered_at ").append("timestamp ").append("NOT NULL").append(",\n")

                    // Constraints
                    .append("\t").append("PRIMARY KEY ( ").append("client_id").append(" )\n")

                    // Store
                    .append(") ").append("ON STORE ").append( RELATIONAL_ADAPTER_UNIQUE_NAME )
                    .toString();

            log.debug("Create Table Statement: \n\n" + polySqlPolyfierClientsTable );
            statement.execute( polySqlPolyfierClientsTable );

            // Create execution_tasks Table
            String polySqlPolyfierExecTasksTable = new StringBuilder()
                    .append("CREATE TABLE ").append( "polyfier.executions " ).append("(\n")

                    // Fields
                    .append("\t").append("exec_id ").append("varchar(36) ").append("NOT NULL ").append(",\n")

                    .append("\t").append("exec_hash ").append("bigint ").append("NOT NULL").append(",\n")
                    .append("\t").append("task_id ").append("varchar(36) ").append("NOT NULL").append(",\n")
                    .append("\t").append("config_id ").append("varchar(36) ").append("NOT NULL").append(",\n")
                    .append("\t").append("client_id ").append("varchar(36) ").append("NOT NULL").append(",\n")
                    // Seed Range
                    .append("\t").append("seed_from ").append("bigint ").append("NOT NULL").append(",\n")
                    .append("\t").append("seed_to ").append("bigint ").append("NOT NULL").append(",\n")
                    // Timestamps
                    .append("\t").append("issued_at ").append("timestamp ").append("NOT NULL").append(",\n")
                    .append("\t").append("completed_at ").append("timestamp ").append("NULL " ).append(",\n")

                    // Constraints
                    .append("\t").append("PRIMARY KEY ( ").append("exec_id").append(" )\n")

                    // Store
                    .append(") ").append("ON STORE ").append( RELATIONAL_ADAPTER_UNIQUE_NAME )
                    .toString();


            log.debug("Create Table Statement: \n\n" + polySqlPolyfierExecTasksTable );
            statement.execute( polySqlPolyfierExecTasksTable );

            // Create polyfier_results Table
            String polySqlPolyfierResultsTable = new StringBuilder()
                    .append("CREATE TABLE ").append( "polyfier.results " ).append("(\n")

                    // Fields
                    .append("\t").append("res_id ").append("varchar(36) ").append("NOT NULL ").append(",\n")
                    // Associated Execution Order
                    .append("\t").append("exec_id ").append("varchar(36) ").append("NOT NULL").append(",\n")
                    // Seed of generated query
                    .append("\t").append("seed ").append("bigint ").append("NOT NULL").append(",\n")
                    // If query completed without issues. If false -> Error not null.
                    .append("\t").append("success ").append("boolean ").append("NOT NULL").append(",\n")
                    // Time of Logging
                    .append("\t").append("received_at ").append("timestamp ").append("NOT NULL").append(",\n")
                    // Error in Case of Failure
                    .append("\t").append("error ").append("varchar ").append("NULL ").append("DEFAULT NULL").append(",\n")
                    // ResultSet, Plans
                    .append("\t").append("result_set ").append("varchar ").append("NULL ").append("DEFAULT NULL").append(",\n")
                    .append("\t").append("logical ").append("varchar ").append("NULL ").append("DEFAULT NULL").append(",\n")
                    .append("\t").append("physical ").append("varchar ").append("NULL ").append("DEFAULT NULL").append(",\n")
                    // Hashes
                    //.append("\t").append("result_set_xor_hash ").append("bigint ").append("NULL ").append("DEFAULT NULL").append(",\n")
                    .append("\t").append("result_set_hash ").append("bigint ").append("NULL ").append("DEFAULT NULL").append(",\n")
                    .append("\t").append("logical_hash ").append("bigint ").append("NULL ").append("DEFAULT NULL").append(",\n")
                    .append("\t").append("physical_hash ").append("bigint ").append("NULL ").append("DEFAULT NULL").append(",\n")
                    // Execution Time
                    .append("\t").append("actual_ms ").append("bigint ").append("NULL ").append("DEFAULT NULL").append(",\n")
                    .append("\t").append("predicted_ms ").append("bigint ").append("NULL ").append("DEFAULT NULL").append(",\n")

                    // Constraints
                    .append("\t").append("PRIMARY KEY ( ").append("res_id").append(" )\n")

                    // Store
                    .append(") ").append("ON STORE ").append( RELATIONAL_ADAPTER_UNIQUE_NAME )
                    .toString();

            log.debug("Create Table: \n\n" + polySqlPolyfierResultsTable );
            statement.execute( polySqlPolyfierResultsTable );


        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void dropTables( Statement statement ) {
        log.debug("Dropping Tables...");
        String polySql = """
                DROP TABLE IF EXISTS %s.%s
        """;
        try {
            statement.execute( polySql.formatted( SCHEMA_NAME, "results" ) );
            statement.execute( polySql.formatted( SCHEMA_NAME, "executions" ) );
            statement.execute( polySql.formatted( SCHEMA_NAME, "tasks" ) );
            statement.execute( polySql.formatted( SCHEMA_NAME, "configurations" ) );
            statement.execute( polySql.formatted( SCHEMA_NAME, "clients" ) );
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    private void dropSchema( Statement statement ) {
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


    private void dropAdapter( Statement statement ) {
        log.debug("Dropping Adapters...");
        String polySql = """
                ALTER ADAPTERS DROP %s
        """;
        try {
            statement.execute( polySql.formatted( RELATIONAL_ADAPTER_UNIQUE_NAME ) );
        } catch ( SQLException e) {
            log.warn("Assuming Adapter does not exist. SQLException: ", e );
        }
    }


}
