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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.MurmurHash2;
import org.apache.commons.lang3.tuple.Pair;
import server.PolyphenyDbProfile;
import server.ServerConfig;
import server.config.*;
import server.generators.PolyphenyDbProfileGenerator;
import server.requests.PolyphenyDbRequest;
import server.responses.PolyphenyDbResponse;

import java.sql.*;
import java.util.*;

@Slf4j
@Getter(AccessLevel.PRIVATE)
public class QueryLogConnection {
    private final QueryLogAdapter queryLogAdapter;
    private final Connection connection;

    public QueryLogConnection( String url, String user, String password ) {
        this.queryLogAdapter = new QueryLogAdapter();
        this.connection = getQueryLogAdapter().connect( url, user, password ).orElseThrow();



        String apiKey = "a-a-a-b-b-b-c-c-c";
        try {
            logInPolyphenyControlClient( getStatement(), apiKey );
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        PolyphenyDbProfileGenerator polyphenyDbProfileGenerator = ServerConfig.getDefaultGenerator();
        SeedsConfig seedsConfig = new SeedsConfig.SeedsBuilder()
                .addSeeds( List.of( 1L, 2L, 3L, 4L, 5L) )
                .addSeeds( List.of( 8L, 9L, 10L, 11L, 12L ) )
                .build();


        try {
            String profileKey = UUID.randomUUID().toString();
            PolyphenyDbProfile profile = polyphenyDbProfileGenerator.createProfile(
                    profileKey,
                    apiKey,
                    seedsConfig
            );

            profileKey = registerPolyphenyDbProfile( getStatement(), apiKey, profile );

            Gson gson = new GsonBuilder().setPrettyPrinting().create();

            PolyphenyDbResponse.GetTask getTask = createTaskFromProfile( getStatement(), profileKey );

            log.debug("Created Task: " + gson.toJson( getTask ) );

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }


    }

    public Statement getStatement() throws SQLException {
        return this.connection.createStatement();
    }

    public void logInPolyphenyControlClient( Statement statement, String apiKey ) throws SQLException {
        log.debug("LOGIN: " + apiKey );
        if ( controlClientIsRegistered( statement, apiKey ) ) {

            if ( controlClientIsLoggedIn( statement, apiKey ) ) {

                log.debug( "Control Client already logged in..." );
                  return;
            }

            log.debug( "Control Client already registered, logging in..." );

            loginControlClient( statement, apiKey );
            return;

        }

        log.debug( "Registering Control Client....." );
        registerControlClient( statement, apiKey );

    }

    public void refreshControlClientStatus( Statement statement, String apiKey, String status ) throws SQLException {
        log.debug("REFRESH: " + apiKey + " : " + status );
        String polySqlRefreshControlClientStatus = """
                UPDATE polyfier.nodes SET status = '%s', last_seen = TIMESTAMP '%s' WHERE (
                    api_key = '%s'
                )
        """;
        update( statement, String.format( polySqlRefreshControlClientStatus, status, Timestamp.valueOf( String.valueOf( System.currentTimeMillis() ) ), apiKey ) );
    }

    public void logOutLostControlClients( Statement statement, Timestamp threshold ) throws SQLException {
        log.debug("AUTO-LOGOUT: " + threshold );
        String polySqlLoginControlClient = """
                UPDATE polyfier.nodes SET logged_in = FALSE, status = 'UNKNOWN' WHERE (
                    last_seen < TIMESTAMP '%s'
                )
        """;
        update( statement, String.format( polySqlLoginControlClient, threshold ) );
    }

    private void registerControlClient(Statement statement, String apiKey ) throws SQLException {
        log.debug("REGISTER: " + apiKey );
        String polySqlRegisterControlClient = """
                INSERT INTO polyfier.nodes (
                    api_key, logged_in, last_seen, status
                ) VALUES (
                    '%s', %s, TIMESTAMP '%s', '%s'
                )
        """;
        insert(
                statement,
                polySqlRegisterControlClient,
                new Object[]{
                        apiKey,
                        true,
                        new Timestamp( System.currentTimeMillis() ),
                        "IDLE"
                }
        );
    }

    public void loginControlClient(Statement statement, String apiKey ) throws SQLException {
        log.debug("LOGIN: " + apiKey );
        String polySqlLoginControlClient = """
                UPDATE polyfier.nodes SET logged_in = TRUE WHERE (
                    api_key = '%s'
                )
        """;
        update( statement, String.format( polySqlLoginControlClient, apiKey ) );
    }

    public boolean controlClientIsRegistered( Statement statement, String apiKey ) throws SQLException {
        log.debug("REGISTERED? " + apiKey );
        String polySqlClientIsRegistered = """
                SELECT * FROM polyfier.nodes WHERE (
                    api_key = '%s'
                )
        """;
        return resultExists( statement, String.format( polySqlClientIsRegistered, apiKey ) );
    }

    public boolean controlClientIsLoggedIn( Statement statement, String apiKey ) throws SQLException {
        log.debug("LOGGED IN? " + apiKey );
        String polySqlClientIsRegistered = """
                SELECT logged_in FROM polyfier.nodes WHERE (
                    api_key = '%s' AND
                    logged_in = TRUE
                )
        """;
        return resultExists( statement, String.format( polySqlClientIsRegistered, apiKey ) );
    }


    public String registerPolyphenyDbProfile(
            Statement statement,
            String apiKey,
            PolyphenyDbProfile polyphenyDbProfile
    ) throws SQLException {
        log.debug("REGISTER PROFILE: \n" + new GsonBuilder().setPrettyPrinting().create().toJson( polyphenyDbProfile ) );
        return registerProfileAndOrder(
                statement,
                apiKey,
                polyphenyDbProfile.getIssuedSeeds().hashAndString(),
                polyphenyDbProfile.getStartConfig().hashAndString(),
                polyphenyDbProfile.getSchemaConfig().hashAndString(),
                polyphenyDbProfile.getQueryConfig().hashAndString(),
                polyphenyDbProfile.getDataConfig().hashAndString(),
                polyphenyDbProfile.getStoreConfig().hashAndString(),
                polyphenyDbProfile.getPartitionConfig().hashAndString()
        );
    }

    private long rehash( Long ... hashes ) {
        StringJoiner sj = new StringJoiner( "&" );
        Arrays.stream(hashes).map(Object::toString).toList().forEach(sj::add);
        return MurmurHash2.hash64( sj.toString() );
    }

    private String registerProfileAndOrder(
            Statement statement,
            String apiKey,
            Pair<Long, String> seedsConfig,
            Pair<Long, String> startConfig,
            Pair<Long, String> schemaConfig,
            Pair<Long, String> queryConfig,
            Pair<Long, String> dataConfig,
            Pair<Long, String> storeConfig,
            Pair<Long, String> partConfig

    ) throws SQLException {
        log.debug("REGISTER PROFILE AND ORDER " + apiKey);
        String polySqlConfigInsert = """
            INSERT INTO polyfier.%s_configs (
                %s_config_hash, %s_config
            ) VALUES (
                %s, '%s'
            )
        """;

        if ( configDoesNotExist(statement, startConfig, "start") ) {
            insert( statement, polySqlConfigInsert, new Object[]{ "start", "start", "start", startConfig.getLeft(), startConfig.getRight() });
        }
        if ( configDoesNotExist(statement, schemaConfig, "schema") ) {
            insert( statement, polySqlConfigInsert, new Object[]{ "schema", "schema", "schema", schemaConfig.getLeft(), schemaConfig.getRight() });
        }
        if ( configDoesNotExist(statement, queryConfig, "query") ) {
            insert( statement, polySqlConfigInsert, new Object[]{ "query", "query", "query", queryConfig.getLeft(), queryConfig.getRight() });
        }
        if ( configDoesNotExist(statement, dataConfig, "data") ) {
            insert( statement, polySqlConfigInsert, new Object[]{ "data", "data", "data", dataConfig.getLeft(), dataConfig.getRight() });
        }
        if ( configDoesNotExist(statement, storeConfig, "store") ) {
            insert( statement, polySqlConfigInsert, new Object[]{ "store", "store", "store", storeConfig.getLeft(), storeConfig.getRight() });
        }
        if ( configDoesNotExist(statement, partConfig, "part") ) {
            insert(statement, polySqlConfigInsert, new Object[]{ "part", "part", "part", partConfig.getLeft(), partConfig.getRight()});
        }

        String polySqlProfileInsert = """
            INSERT INTO polyfier.profiles (
                profile, profile_hash, start_config_hash, schema_config_hash, query_config_hash, data_config_hash, store_config_hash, part_config_hash
            ) VALUES (
                '%s', %s, %s, %s, %s, %s, %s, %s
            )
        """;


        final List<String> profileKey = new LinkedList<>();
        getProfilePk(
                statement,
                startConfig.getLeft(),
                schemaConfig.getLeft(),
                queryConfig.getLeft(),
                dataConfig.getLeft(),
                storeConfig.getLeft(),
                partConfig.getLeft()
        ).ifPresentOrElse(
                profileKey::add,
                () -> profileKey.add( UUID.randomUUID().toString() )
        );


        insert(
                statement,
                polySqlProfileInsert,
                new Object[]{
                        profileKey.get( 0 ),
                        rehash( // Rehashing for easy overall comparison...
                            startConfig.getLeft(),
                            schemaConfig.getLeft(),
                            queryConfig.getLeft(),
                            dataConfig.getLeft(),
                            storeConfig.getLeft(),
                            partConfig.getLeft()
                        ),
                        startConfig.getLeft(),
                        schemaConfig.getLeft(),
                        queryConfig.getLeft(),
                        dataConfig.getLeft(),
                        storeConfig.getLeft(),
                        partConfig.getLeft()
                }
        );

        String polySqlOrderInsert = """
                INSERT INTO polyfier.orders (
                    order_id, api_key, profile, seeds, issued_at, completed_at
                ) VALUES (
                    '%s', '%s', '%s', '%s', TIMESTAMP '%s', %s
                )
        """;

        insert(
                statement,
                polySqlOrderInsert,
                new Object[]{
                        UUID.randomUUID().toString(),
                        apiKey,
                        profileKey.get( 0 ),
                        seedsConfig.getRight(),
                        new Timestamp( System.currentTimeMillis() ),
                        "NULL"
                }
        );

        return profileKey.get( 0 );

    }

    private boolean configDoesNotExist(Statement statement, Pair<Long, String> config, String configTrace ) throws SQLException {
        log.debug("CONFIG EXISTS? " + configTrace + " HASH " + config.getLeft() );
        String polySqlConfigExists = """
            SELECT * FROM polyfier.%s_configs WHERE (
                %s_config_hash = %s
            )
        """;
        return ! resultExists(statement, String.format(polySqlConfigExists, configTrace, configTrace, config.getLeft()));
    }

    private Optional<String> getProfilePk(
            Statement statement,
            long startConfigHash,
            long schemaConfigHash,
            long queryConfigHash,
            long dataConfigHash,
            long storeConfigHash,
            long partConfigHash

    ) throws SQLException {
        String polySqlGetProfile = """
            SELECT profile FROM polyfier.profiles WHERE (
                profile_hash = %s
            )
        """;

        ResultSet resultSet = handleResult(
                statement,
                polySqlGetProfile,
                new Object[]{
                        rehash(
                                startConfigHash,
                                schemaConfigHash,
                                queryConfigHash,
                                dataConfigHash,
                                storeConfigHash,
                                partConfigHash
                        )
                }
        );

        if ( ! resultSet.next() ) {
            resultSet.close();
            return Optional.empty();
        }

        String result = resultSet.getString( "profile" );
        resultSet.close();
        return Optional.of( result );
    }

    public boolean nodeIsIdle( final Statement statement, final String apiKey ) throws SQLException {
        final String polySqlNodeIsIdle = """
            SELECT * FROM polyfier.nodes WHERE (
                api_key = '%s' AND
                status = 'IDLE'
            )
        """;
        return resultExists( statement, String.format( polySqlNodeIsIdle, apiKey ) );
    }

    public boolean nodeIsLost( final Statement statement, final String apiKey ) throws SQLException {
        final String polySqlNodeIsIdle = """
            SELECT * FROM polyfier.nodes WHERE (
                api_key = '%s' AND
                status = 'UNKNOWN'
            )
        """;
        return resultExists( statement, String.format( polySqlNodeIsIdle, apiKey ) );
    }

    public PolyphenyDbResponse.GetTask createTaskFromProfile( final Statement statement, String clientId ) throws SQLException {
        final String polySqlCompositionTask = """
            SELECT schema_config, data_config, query_config, store_config, part_config FROM polyfier.profiles, polyfier.schema_configs, polyfier.data_configs, polyfier.query_configs, polyfier.store_configs, polyfier.part_configs WHERE (
                profiles.profile = '%s' AND
                profiles.schema_config_hash = schema_configs.schema_config_hash AND
                profiles.data_config_hash = data_configs.data_config_hash AND
                profiles.query_config_hash = query_configs.query_config_hash AND
                profiles.store_config_hash = store_configs.store_config_hash AND
                profiles.part_config_hash = part_configs.part_config_hash
            )
        """;

        ResultSet resultSet = handleResult( statement, String.format( polySqlCompositionTask, clientId ) );
        if ( ! resultSet.next() ) {
            throw new RuntimeException("Task does not exists: " + clientId);
        }

        Gson gson = new GsonBuilder().create();

        PolyphenyDbResponse.GetTask getTask = new PolyphenyDbResponse.GetTask(
                gson.fromJson( resultSet.getString("schema_config"), SchemaConfig.class ),
                gson.fromJson( resultSet.getString("data_config"), DataConfig.class ),
                gson.fromJson( resultSet.getString("query_config"), QueryConfig.class ),
                gson.fromJson( resultSet.getString("store_config"), StoreConfig.class ),
                gson.fromJson( resultSet.getString("part_config"), PartitionConfig.class )
        );

        resultSet.close();
        return getTask;
    }


    private boolean resultExists( Statement statement, String query ) throws SQLException {
        statement.execute( query );
        ResultSet resultSet = statement.getResultSet();
        boolean exists = resultSet.next();
        resultSet.close();
        return exists;
    }

    private void update( Statement statement, String query ) throws SQLException {
        statement.execute( query );
    }

    private void insert( Statement statement, String query, Object[] args ) throws SQLException {
        statement.execute( String.format( query, args ) );
    }

    private ResultSet handleResult( Statement statement, String query, Object[] args ) throws SQLException {
        return handleResult( statement, String.format( query, args ) );
    }

    private ResultSet handleResult( Statement statement, String query ) throws SQLException {
        statement.execute( query );
        return statement.getResultSet();
    }

    public boolean registerResult(
            Statement statement,
            PolyphenyDbRequest.Result result
    ) throws SQLException {

        Pair<Long, String> errorConfig = new ErrorConfig( result.getError() ).hashAndString();
        Pair<Long, String> logicalPlanConfig = new LogicalPlanConfig( result.getLogical() ).hashAndString();
        Pair<Long, String> physicalPlanConfig = new PhysicalPlanConfig( result.getPhysical() ).hashAndString();

        String polySqlConfigInsert = """
            INSERT INTO polyfier.%s_configs (
                %s_config_hash, %s_config
            ) VALUES (
                %s, '%s'
            )
        """;

        if ( configDoesNotExist( statement, errorConfig, "error") ) {
            insert( statement, polySqlConfigInsert, new Object[]{ "error", "error", "error", errorConfig.getLeft(), errorConfig.getRight() });
        }
        if ( configDoesNotExist(statement, logicalPlanConfig, "logical") ) {
            insert( statement, polySqlConfigInsert, new Object[]{ "logical", "logical", "logical", logicalPlanConfig.getLeft(), logicalPlanConfig.getRight() });
        }
        if ( configDoesNotExist(statement, physicalPlanConfig, "physical") ) {
            insert( statement, polySqlConfigInsert, new Object[]{ "physical", "physical", "physical", physicalPlanConfig.getLeft(), physicalPlanConfig.getRight() });
        }

        String polySqlRegisterResult = """
            INSERT INTO polyfier.results (
                result_id, order_id, seed, success, received_at, error_hash, result_set_hash, logical_hash, physical_hash, actual_ms, predicted_ms
            ) VALUES (
                '%s', '%s', %s, %s, TIMESTAMP '%s', %s, %s, %s, %s, %s, %s
            )
        """;

        try {
            return statement.execute( String.format(
                    polySqlRegisterResult,
                    UUID.randomUUID(),
                    result.getOrderId(),
                    result.getSeed(),
                    result.getSuccess(),
                    new Timestamp( System.currentTimeMillis() ),
                    errorConfig.getLeft(),
                    result.getResultSetHash(),
                    logicalPlanConfig.getLeft(),
                    physicalPlanConfig.getLeft(),
                    result.getActual(),
                    result.getPredicted()
            ) );

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    public boolean isActive() {
        try {
            return this.connection.isValid( 5000 );
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static long valCountXorHash(  String resultSet ) {
        /*
            There is a way of solving the ResultSet hash-problem:

            Prerequisite, need all rows as separate strings...

            0.1 Assuming that the order of values in a row is relevant.
            0.2 Assuming that the order of rows is irrelevant.

            1. Accumulator acc
            2. Hash all Rows independently.
            3. Sum all distinct Hashes. Separately.
            4. Stream the resulting distinct hashes.
            5. Reduce the stream with a XOR operation.
                -> That's why we summed the value counts, XOR cancels out duplicates.
                -> But with XOR the order does not matter.

            -> Produces the same hash for differently ordered rows
            -> Does not produce the same hash if the value counts of distinct rows were different.

            Now.. POC &...
            Proof:
            // Todo

         */
        return 0L;
    }

}

//

// ALTER ADAPTERS DROP "asas"
