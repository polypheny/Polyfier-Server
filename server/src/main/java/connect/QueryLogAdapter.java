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
import java.util.StringJoiner;

@Slf4j
public class QueryLogAdapter {
    private static final java.sql.Driver DRIVER = new Driver();
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
            Connection connection = DRIVER.connect( url, props );
            return Optional.of( connection );
        } catch ( SQLException e ) {
            throw new RuntimeException(e);
        }
    }

    public void setUp( String url, String user, String password ) {
        Properties props = new Properties();
        props.setProperty( "user", user );
        props.setProperty( "password", password );
        try {
            Driver driver = new Driver();
            Connection connection = DRIVER.connect( url, props );
            configure( connection );
            connection.commit();
        } catch ( SQLException e ) {
            throw new RuntimeException(e);
        }
    }

    public boolean isConfigured() {

        return false;
    }

    public boolean resetParameter() {
        // Todo implement reset -> UI
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
        } catch (SQLException e) {
            log.warn("Adapter is probably already configured.");
        }
        try {
            statement.execute( polySql.formatted(
                    DOCUMENT_ADAPTER_UNIQUE_NAME,
                    DOCUMENT_ADAPTER,
                    DOCUMENT_ADAPTER_CONFIG
            ) );
        } catch (SQLException e) {
            log.warn("Adapter is probably already configured.");
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
            String schemaConfigs = new StringBuilder()
                    .append("CREATE TABLE ").append( "polyfier.schema_configs " ).append("(\n")

                    // Fields
                    .append("\t").append("schema_config_hash ").append("bigint ").append("NOT NULL ").append(",\n")
                    .append("\t").append("schema_config ").append("varchar ").append("NOT NULL").append(",\n")

                    // Constraints
                    .append("\t").append("PRIMARY KEY ( ").append("schema_config_hash").append(" )\n")

                    // Store
                    .append(") ").append("ON STORE ").append( DOCUMENT_ADAPTER_UNIQUE_NAME )
                    .toString();

            log.debug("Create Table \n\n" + schemaConfigs );
            statement.execute( schemaConfigs );

            String queryConfigs = new StringBuilder()
                .append("CREATE TABLE ").append( "polyfier.query_configs " ).append("(\n")

                // Fields
                .append("\t").append("query_config_hash ").append("bigint ").append("NOT NULL ").append(",\n")
                .append("\t").append("query_config ").append("varchar ").append("NOT NULL").append(",\n")

                // Constraints
                .append("\t").append("PRIMARY KEY ( ").append("query_config_hash").append(" )\n")

                // Store
                .append(") ").append("ON STORE ").append( DOCUMENT_ADAPTER_UNIQUE_NAME )
                .toString();

            log.debug("Create Table \n\n" + queryConfigs );
            statement.execute( queryConfigs );

            String dataConfigs = new StringBuilder()
                    .append("CREATE TABLE ").append( "polyfier.data_configs " ).append("(\n")

                    // Fields
                    .append("\t").append("data_config_hash ").append("bigint ").append("NOT NULL ").append(",\n")
                    .append("\t").append("data_config ").append("varchar ").append("NOT NULL").append(",\n")

                    // Constraints
                    .append("\t").append("PRIMARY KEY ( ").append("data_config_hash").append(" )\n")

                    // Store
                    .append(") ").append("ON STORE ").append( DOCUMENT_ADAPTER_UNIQUE_NAME )
                    .toString();

            log.debug("Create Table \n\n" + dataConfigs );
            statement.execute( dataConfigs );

            String errors = new StringBuilder()
                    .append("CREATE TABLE ").append( "polyfier.error_configs " ).append("(\n")

                    // Fields
                    .append("\t").append("error_config_hash ").append("bigint ").append("NOT NULL ").append(",\n")
                    .append("\t").append("error_config ").append("varchar ").append("NOT NULL").append(",\n")

                    // Constraints
                    .append("\t").append("PRIMARY KEY ( ").append("error_config_hash").append(" )\n")

                    // Store
                    .append(") ").append("ON STORE ").append( DOCUMENT_ADAPTER_UNIQUE_NAME )
                    .toString();

            log.debug("Create Table \n\n" + errors );
            statement.execute( errors );

            String physicalPlansDocument = new StringBuilder()
                    .append("CREATE TABLE ").append( "polyfier.physical_configs " ).append("(\n")

                    // Fields
                    .append("\t").append("physical_config_hash ").append("bigint ").append("NOT NULL ").append(",\n")
                    .append("\t").append("physical_config ").append("varchar ").append("NOT NULL").append(",\n")

                    // Constraints
                    .append("\t").append("PRIMARY KEY ( ").append("physical_config_hash").append(" )\n")

                    // Store
                    .append(") ").append("ON STORE ").append( DOCUMENT_ADAPTER_UNIQUE_NAME )
                    .toString();

            log.debug("Create Table \n\n" + physicalPlansDocument );
            statement.execute( physicalPlansDocument );

            String logicalPlans = new StringBuilder()
                    .append("CREATE TABLE ").append( "polyfier.logical_configs " ).append("(\n")

                    // Fields
                    .append("\t").append("logical_config_hash ").append("bigint ").append("NOT NULL ").append(",\n")
                    .append("\t").append("logical_config ").append("varchar ").append("NOT NULL").append(",\n")

                    // Constraints
                    .append("\t").append("PRIMARY KEY ( ").append("logical_config_hash").append(" )\n")

                    // Store
                    .append(") ").append("ON STORE ").append( DOCUMENT_ADAPTER_UNIQUE_NAME )
                    .toString();

            log.debug("Create Table \n\n" + logicalPlans );
            statement.execute( logicalPlans );

            String storeConfigs = new StringBuilder()
                    .append("CREATE TABLE ").append( "polyfier.store_configs " ).append("(\n")

                    // Fields
                    .append("\t").append("store_config_hash ").append("bigint ").append("NOT NULL ").append(",\n")
                    .append("\t").append("store_config ").append("varchar ").append("NOT NULL").append(",\n")

                    // Constraints
                    .append("\t").append("PRIMARY KEY ( ").append("store_config_hash").append(" )\n")

                    // Store
                    .append(") ").append("ON STORE ").append( DOCUMENT_ADAPTER_UNIQUE_NAME )
                    .toString();

            log.debug("Create Table \n\n" + storeConfigs );
            statement.execute( storeConfigs );

            String partConfigs = new StringBuilder()
                    .append("CREATE TABLE ").append( "polyfier.part_configs " ).append("(\n")

                    // Fields
                    .append("\t").append("part_config_hash ").append("bigint ").append("NOT NULL ").append(",\n")
                    .append("\t").append("part_config ").append("varchar ").append("NOT NULL").append(",\n")

                    // Constraints
                    .append("\t").append("PRIMARY KEY ( ").append("part_config_hash").append(" )\n")

                    // Store
                    .append(") ").append("ON STORE ").append( DOCUMENT_ADAPTER_UNIQUE_NAME )
                    .toString();

            log.debug("Create Table \n\n" + partConfigs );
            statement.execute( partConfigs );

            String startConfigs = new StringBuilder()
                    .append("CREATE TABLE ").append( "polyfier.start_configs " ).append("(\n")

                    // Fields
                    .append("\t").append("start_config_hash ").append("bigint ").append("NOT NULL ").append(",\n")
                    .append("\t").append("start_config ").append("varchar ").append("NOT NULL").append(",\n")

                    // Constraints
                    .append("\t").append("PRIMARY KEY ( ").append("start_config_hash").append(" )\n")

                    // Store
                    .append(") ").append("ON STORE ").append( DOCUMENT_ADAPTER_UNIQUE_NAME )
                    .toString();


            log.debug("Create Table \n\n" + startConfigs );
            statement.execute( startConfigs );

            String pctrl = new StringBuilder()
                    .append("CREATE TABLE ").append( "polyfier.pctrl " ).append("(\n")

                    .append("\t").append("pctrlKey ").append("varchar(36) ").append("NOT NULL").append(",\n")
                    .append("\t").append("branch ").append("varchar ").append("NOT NULL").append(",\n")
                    .append("\t").append("registeredAt ").append("timestamp ").append("NOT NULL").append(",\n")

                    // Constraints
                    .append("\t").append("PRIMARY KEY ( ").append("pctrlKey").append(" )\n")

                    // Store
                    .append(") ").append("ON STORE ").append( RELATIONAL_ADAPTER_UNIQUE_NAME )
                    .toString();

            log.debug("Create Table \n\n" + pctrl );
            statement.execute( pctrl );

            String pdb = new StringBuilder()
                    .append("CREATE TABLE ").append( "polyfier.pdb " ).append("(\n")

                    // Fields
                    .append("\t").append("pdbKey ").append("varchar(36) ").append("NOT NULL ").append(",\n")
                    .append("\t").append("branch ").append("varchar ").append("NOT NULL").append(",\n")
                    .append("\t").append("registeredAt ").append("timestamp ").append("NOT NULL").append(",\n")
                    .append("\t").append("pctrlKey ").append("varchar(36) ").append("NOT NULL ").append(",\n")
                    .append("\t").append("schema_config_hash ").append("bigint ").append("NOT NULL").append(",\n")
                    .append("\t").append("data_config_hash ").append("bigint ").append("NOT NULL").append(",\n")
                    .append("\t").append("query_config_hash ").append("bigint ").append("NOT NULL").append(",\n")
                    .append("\t").append("store_config_hash ").append("bigint ").append("NOT NULL").append(",\n")
                    .append("\t").append("start_config_hash ").append("bigint ").append("NOT NULL").append(",\n")
                    .append("\t").append("part_config_hash ").append("bigint ").append("NOT NULL").append(",\n")

                    // Constraints
                    .append("\t").append("PRIMARY KEY ( ").append("pdbKey").append(" )\n")

                    // Store
                    .append(") ").append("ON STORE ").append( RELATIONAL_ADAPTER_UNIQUE_NAME )
                    .toString();

            log.debug("Create Table \n\n" + pdb );
            statement.execute( pdb );

            // Create polyfier_results Table
            String results = new StringBuilder()
                    .append("CREATE TABLE ").append( "polyfier.results " ).append("(\n")

                    // Fields
                    .append("\t").append("resultKey ").append("varchar(36) ").append("NOT NULL ").append(",\n")
                    .append("\t").append("pdbKey ").append("varchar(36) ").append("NOT NULL").append(",\n")
                    .append("\t").append("seed ").append("bigint ").append("NOT NULL").append(",\n")
                    .append("\t").append("success ").append("boolean ").append("NOT NULL").append(",\n")
                    .append("\t").append("receivedAt ").append("timestamp ").append("NOT NULL").append(",\n")
                    .append("\t").append("errorHash ").append("bigint ").append("NULL ").append("DEFAULT NULL").append(",\n")
                    .append("\t").append("resultSetHash ").append("bigint ").append("NULL ").append("DEFAULT NULL").append(",\n")
                    .append("\t").append("logicalHash ").append("bigint ").append("NULL ").append("DEFAULT NULL").append(",\n")
                    .append("\t").append("physicalHash ").append("bigint ").append("NULL ").append("DEFAULT NULL").append(",\n")
                    .append("\t").append("execTime ").append("bigint ").append("NULL ").append("DEFAULT NULL").append(",\n")

                    // Constraints
                    .append("\t").append("PRIMARY KEY ( ").append("resultKey").append(" )\n")

                    // Store
                    .append(") ").append("ON STORE ").append( RELATIONAL_ADAPTER_UNIQUE_NAME )
                    .toString();

            log.debug("Create Table: \n\n" + results );
            statement.execute( results );


            String constraint;

            constraint = """
                ALTER TABLE polyfier.%s ADD CONSTRAINT %s FOREIGN KEY ( %s ) REFERENCES polyfier.%s ( %s ) ON UPDATE RESTRICT ON DELETE NONE
            """;

            // Results
            statement.execute( String.format( constraint, "results", "res_fk1", "pdbKey", "pdb", "pdbKey" ) );
            statement.execute( String.format( constraint, "results", "res_fk2", "errorHash", "error_configs", "error_config_hash" ) );
            statement.execute( String.format( constraint, "results", "res_fk3", "logicalHash", "logical_configs", "logical_config_hash" ) );
            statement.execute( String.format( constraint, "results", "res_fk4", "physicalHash", "physical_configs", "physical_config_hash" ) );


            // Results
            statement.execute( String.format( constraint, "pdb", "pdb_fk1", "pctrlKey", "pctrl", "pctrlKey" ) );
            statement.execute( String.format( constraint, "pdb", "pro_fk1", "start_config_hash", "start_configs", "start_config_hash" ) );
            statement.execute( String.format( constraint, "pdb", "pro_fk2", "schema_config_hash", "schema_configs", "schema_config_hash" ) );
            statement.execute( String.format( constraint, "pdb", "pro_fk3", "query_config_hash", "query_configs", "query_config_hash" ) );
            statement.execute( String.format( constraint, "pdb", "pro_fk4", "data_config_hash", "data_configs", "data_config_hash" ) );
            statement.execute( String.format( constraint, "pdb", "pro_fk5", "store_config_hash", "store_configs", "store_config_hash" ) );
            statement.execute( String.format( constraint, "pdb", "pro_fk6", "part_config_hash", "part_configs", "part_config_hash" ) );


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
            statement.execute( polySql.formatted( SCHEMA_NAME, "pdb" ) );
            statement.execute( polySql.formatted( SCHEMA_NAME, "pctrl" ) );
            statement.execute( polySql.formatted( SCHEMA_NAME, "query_config" ) );
            statement.execute( polySql.formatted( SCHEMA_NAME, "data_configs" ) );
            statement.execute( polySql.formatted( SCHEMA_NAME, "errors" ) );
            statement.execute( polySql.formatted( SCHEMA_NAME, "phy_plans" ) );
            statement.execute( polySql.formatted( SCHEMA_NAME, "log_plans" ) );
            statement.execute( polySql.formatted( SCHEMA_NAME, "store_configs" ) );
            statement.execute( polySql.formatted( SCHEMA_NAME, "part_configs" ) );
            statement.execute( polySql.formatted( SCHEMA_NAME, "start_configs" ) );
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

    private static class TableCreator {
        private final Statement statement;
        private static final String SQL = """
                CREATE TABLE %s.%s (
                %s,
                PRIMARY KEY ( %s )
                ) ON STORE %s
        """;
        private static final String COLUMN = "\t%s %s %s";
        private static final String COLUMN_DEFAULT = "\t%s %s %s %s";

        private String schema;
        private String table;
        private StringJoiner columns;
        private String primary;
        private String store;


        public TableCreator schema( String name ) {
            schema = name;
            return this;
        }

        public TableCreator table( String name ) {
            table = name;
            return this;
        }

        public TableCreator column( String name, String type, String nullability ) {
            columns.add( String.format(COLUMN, name, type, nullability) );
            return this;
        }

        public TableCreator column( String name, String type, String nullability, String def ) {
            columns.add( String.format(COLUMN_DEFAULT, name, type, nullability, def) );
            return this;
        }


        public TableCreator store( String name ) {
            store = name;
            return this;
        }

        public TableCreator primary( String name ) {
            primary = name;
            return this;
        }

        public TableCreator( Statement statement ) {
            this.columns = new StringJoiner(",\n");
            this.statement = statement;
        }

        public void create() throws SQLException {
            statement.execute( String.format( SQL, schema, table, columns.toString(), primary, store ) );
        }

    }


}
