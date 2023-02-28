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
import io.javalin.http.Context;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.MurmurHash2;

import java.sql.*;
import java.util.*;

@Slf4j
@Getter(AccessLevel.PRIVATE)
public class QueryLogConnection {
    private QueryLogAdapter queryLogAdapter;
    private Connection connection;

    public QueryLogConnection( String url, String user, String password ) {
        this.queryLogAdapter = new QueryLogAdapter();
        this.connection = getQueryLogAdapter().connect( url, user, password ).orElseThrow();

        // Debug Testing

        try {
            Statement statement = connection.createStatement();

            List<Object> config = registerConfiguration( null, statement, "A", "B", "C");

            HashMap<String, String> clientInfo = new HashMap<>();

            clientInfo.put("branch", "mock");
            clientInfo.put("version", "v1.0");

            List<Object> client = registerClient( clientInfo, statement );

            List<Object> task = registerTask( null, statement,"{}", "{}", "default");

            List<Object> executionTask = registerExecutionTask(
                    null,
                    statement,
                    (String) task.get(0),
                    (String) client.get(0),
                    (String) config.get(0),
                    0,
                    100
            );

            String execId = (String) executionTask.get( 0 );

            boolean receivedResult = registerResult(
                    null,
                    statement,
                    execId,
                    0,
                    true,
                    "",
                    "{}",
                    "{}",
                    "{}",
                    100,
                    -1
            );

            if ( receivedResult ) {
                log.debug("Hurray!");
            }


        } catch (SQLException e) {
            throw new RuntimeException(e);
        }



    }

    private Optional<List<Object>> getConfiguration(Statement statement, List<Long> hashes ) {
        String polySqlConfigurationExists = """
                SELECT * FROM polyfier.configurations WHERE (
                    store_config_hash = %s AND
                    partition_config_hash = %s AND
                    start_config_hash = %s
                )
        """;

        try {
            log.debug("Executing...");
            log.debug( polySqlConfigurationExists );
            statement.execute( String.format(
                    polySqlConfigurationExists,
                    hashes.get( 0 ),
                    hashes.get( 1 ),
                    hashes.get( 2 )
            ) );

            log.debug("Get Result set...");
            ResultSet resultSet = statement.getResultSet();

            log.debug("Converting...");

            // Todo Feature not supported Exception for hsqldb store (?) when using resultSet.first() <-
            if ( resultSet.next() ) {
                return Optional.of( List.of(
                        resultSet.getString( 1 ),
                        resultSet.getString( 2 ),
                        resultSet.getString( 3 ),
                        resultSet.getString( 4 ),
                        resultSet.getLong( 5 ),
                        resultSet.getLong( 6 ),
                        resultSet.getLong( 7 )
                ) );
            } else {
                return Optional.empty();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public List<Object> registerConfiguration( Context ctx, Statement statement, String storeConfig, String partitionConfig, String startConfig ) {

        List<Long> hashes = List.of(
                MurmurHash2.hash64( storeConfig ),
                MurmurHash2.hash64( partitionConfig ),
                MurmurHash2.hash64( startConfig )
        );

        Optional<List<Object>> configuration = getConfiguration( statement, hashes );

        // If we already stored a configuration we can reuse it.
        if ( configuration.isPresent() ) {
            return configuration.get();
        }

        // Otherwise we will store a new configuration.
        String polySqlInsertNewConfiguration = """
                INSERT INTO polyfier.configurations (
                    config_id, store_config, partition_config, start_config, store_config_hash, partition_config_hash, start_config_hash
                ) VALUES (
                    '%s', '%s', '%s', '%s', %s, %s, %s
                )
        """;
        try {
            statement.execute( String.format(
                    polySqlInsertNewConfiguration,
                    UUID.randomUUID(),
                    storeConfig,
                    partitionConfig,
                    startConfig,
                    hashes.get( 0 ),
                    hashes.get( 1 ),
                    hashes.get( 2 )
            ) );
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return getConfiguration( statement, hashes ).orElseThrow();
    }

    public Optional<List<Object>> getClient( Statement statement, String uuid ) {
        String polySqlGetClient = """
            SELECT * FROM polyfier.clients WHERE (
                clients.client_id = '%s'
            )
        """;

        try {
            statement.execute( String.format(polySqlGetClient, uuid ) );

            ResultSet resultSet = statement.getResultSet();

            if ( resultSet.next() ) {
                return Optional.of( List.of(
                        resultSet.getString( 1 ),
                        resultSet.getString( 2 ),
                        resultSet.getString( 3 ),
                        resultSet.getTimestamp( 4 )
                ) );
            } else {
                return Optional.empty();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public List<Object> registerClient( Context ctx, Statement statement ) {
        return registerClient( (HashMap<String, String>) new GsonBuilder().create().fromJson( ctx.body(), HashMap.class), statement );
    }

    public List<Object> registerClient( HashMap<String, String> clientInfo, Statement statement ) {

        // If the client explicitly suggests it is already registered, for example if the connection was broken,
        // we can look for this client.
        if ( clientInfo.containsKey("register_new") && ! Boolean.parseBoolean( clientInfo.get( "register_new" ) ) ) {
            if ( ! clientInfo.containsKey( "uuid" ) ) {
                throw new IllegalArgumentException( "Invalid request parameters, no client ID provided." );
            }

            Optional<List<Object>> client = getClient( statement, clientInfo.get( "uuid" ) );

            return client.orElseThrow();

        }

        // Else we register a new client.

        String polySqlInsertClient = """
            INSERT INTO polyfier.clients (
                client_id, branch, version, registered_at
            ) VALUES (
                '%s', '%s', '%s', TIMESTAMP '%s'
            )
        """;

        String polySqlGetId = "SELECT MAX(clients.client_id) FROM polyfier.clients";

        try {
            statement.execute( String.format( polySqlInsertClient, UUID.randomUUID(), clientInfo.get("branch"), clientInfo.get("version"), new Timestamp( System.currentTimeMillis() ) ) );

            statement.execute( polySqlGetId );
            ResultSet resultSet = statement.getResultSet();

            if ( ! resultSet.next() ) {
                throw new RuntimeException("Insertion Error, could not retrieve inserted row.");
            }

            String clientId = resultSet.getString(  1 );

            return getClient( statement, clientId ).orElseThrow();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    private Optional<List<Object>> getTask(Statement statement, List<Long> hashes, String schemaType ) {
        String polySqlConfigurationExists = """
                SELECT * FROM polyfier.tasks WHERE (
                    tasks.query_config_hash = %s AND
                    tasks.data_config_hash = %s AND
                    tasks.schema_type = '%s'
                )
        """;

        try {
            statement.execute( String.format( polySqlConfigurationExists, hashes.get( 0 ), hashes.get( 1 ), schemaType ) );

            ResultSet resultSet = statement.getResultSet();

            if ( resultSet.next() ) {
                return Optional.of( List.of(
                        resultSet.getString( 1 ),
                        resultSet.getString( 2 ),
                        resultSet.getString( 3 ),
                        resultSet.getLong( 4 ),
                        resultSet.getLong( 5 ),
                        resultSet.getString( 6 )
                ) );
            } else {
                return Optional.empty();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public List<Object> registerTask( Context ctx, Statement statement, String queryConfig, String dataConfig, String schemaType ) {
        List<Long> hashes = List.of(
                MurmurHash2.hash64( queryConfig ),
                MurmurHash2.hash64( dataConfig ),
                MurmurHash2.hash64( schemaType )
        );

        Optional<List<Object>> task = getTask( statement, hashes, schemaType );

        if ( task.isPresent() ) {
            return task.get();
        }

        String polySqlRegisterTask = """
            INSERT INTO polyfier.tasks (
                task_id, query_config, data_config, query_config_hash, data_config_hash, schema_type
            ) VALUES (
                '%s', '%s', '%s', %s, %s, '%s'
            )
        """;

        try {
            statement.execute( String.format(
                    polySqlRegisterTask,
                    UUID.randomUUID(),
                    queryConfig,
                    dataConfig,
                    hashes.get( 0 ),
                    hashes.get( 1 ),
                    schemaType
            ) );

            return getTask( statement, hashes, schemaType ).orElseThrow();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    public Optional<List<Object>> getExecutionTask( Statement statement, long hash ) {
        String polySqlGetExecutionTaskByHash = """
            SELECT * FROM polyfier.executions WHERE (
                executions.exec_hash = %s
            )
        """;

        try {
            statement.execute( String.format( polySqlGetExecutionTaskByHash, hash) );

            ResultSet resultSet = statement.getResultSet();

            if ( resultSet.next() ) {
                Optional<List<Object>> result =  Optional.of( List.of(
                        resultSet.getString( 1 ),
                        resultSet.getLong( 2 ),
                        resultSet.getString( 3 ),
                        resultSet.getString( 4 ),
                        resultSet.getString( 5 ),
                        resultSet.getLong( 6 ),
                        resultSet.getLong( 7 ),
                        resultSet.getTimestamp( 8 ),
                        ( resultSet.getTimestamp( 9 ) != null ) ? Optional.of( resultSet.getTimestamp( 9 ) ) : Optional.empty() // Workaround
                ) );
                resultSet.close();
                return result;
            } else {
                return Optional.empty();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private long executionTaskHash(  String taskId, String clientId, String configId, long seedFrom, long seedTo  ) {
        return MurmurHash2.hash64( ( taskId + clientId + configId + seedFrom + ":" + seedTo ) );
    }

    public List<Object> registerExecutionTask( Context ctx, Statement statement, String taskId, String clientId, String configId, long seedFrom, long seedTo ) {
        long hash = executionTaskHash( taskId, clientId, configId, seedFrom, seedTo );

        Optional<List<Object>> executionTask = getExecutionTask( statement, hash );

        if ( executionTask.isPresent() ) { // Execution Task with given seeds, client, config has already been issued.
            // Check if already completed...
            if ( ((Optional<Object>) executionTask.get().get( 7 )).isPresent() ) { // Checks Timestamp completed_at
                // Todo Handle incomplete execution task

                // 1. Lookup which queries have arrived in results table.

                // Todo

                // 2. Update first execution task to be completed with new seedTo or delete if no queries received.

                // Todo

                // 3. Issue new execution task with new seedFrom.

                // Todo

                long delta = 0; // alter
                seedFrom += delta;
                hash = executionTaskHash( taskId, clientId, configId, seedFrom, seedTo );

            } else {
                throw new IllegalArgumentException("Trying to issue same task twice.");
            }

        }

        String polySqlRegisterExecutionTask = """
            INSERT INTO polyfier.executions (
                exec_id, exec_hash, task_id, config_id, client_id, seed_from, seed_to, issued_at, completed_at
            ) VALUES (
                '%s', %s, '%s', '%s', '%s', %s, %s, TIMESTAMP '%s', NULL
            )
        """;

        try {
            statement.execute( String.format(
                    polySqlRegisterExecutionTask,
                    UUID.randomUUID(),
                    hash,
                    taskId,
                    clientId,
                    configId,
                    seedFrom,
                    seedTo,
                    new Timestamp( System.currentTimeMillis() )
            ) );

            return getExecutionTask( statement, hash ).orElseThrow();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    public boolean registerResult(
            Context ctx,
            Statement statement,
            String execId,
            long seed,
            boolean success,
            String error,
            String resultSet,
            String logical,
            String physical,
            long actual,
            long predicted ) {

        String polySqlRegisterResult = """
            INSERT INTO polyfier.results (
                res_id, exec_id, seed, success, received_at, error, result_set, logical, physical, result_set_hash, logical_hash, physical_hash, actual_ms, predicted_ms
            ) VALUES (
                '%s', '%s', %s, %s, TIMESTAMP '%s', '%s', '%s', '%s', '%s', %s, %s, %s, %s, %s
            )
        """;

        try {
            return statement.execute( String.format(
                    polySqlRegisterResult,
                    UUID.randomUUID(),
                    execId,
                    seed,
                    success,
                    new Timestamp(System.currentTimeMillis()),
                    error,
                    resultSet,
                    logical,
                    physical,
                    MurmurHash2.hash64( resultSet ),
                    MurmurHash2.hash64( logical ),
                    MurmurHash2.hash64( physical ),
                    actual,
                    predicted
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

    private static long valCountXorHash( String resultSet ) {
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
