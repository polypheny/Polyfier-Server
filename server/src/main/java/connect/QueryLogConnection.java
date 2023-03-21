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
import org.apache.commons.lang3.tuple.Pair;
import server.config.*;
import server.generators.Profile;
import server.generators.ProfileGenerator;
import server.requests.PolyphenyDbRequest;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

@Slf4j
@Getter(AccessLevel.PRIVATE)
public class QueryLogConnection {
    private final QueryLogAdapter queryLogAdapter;
    private final Connection connection;

    public QueryLogConnection( String url, String user, String password ) {
        this.queryLogAdapter = new QueryLogAdapter();
        this.connection = getQueryLogAdapter().connect( url, user, password ).orElseThrow();
        PolySQL.setConnection( this.connection );
        // testInserts();
    }

    private void testInserts() {
        String apiKey = "i_am_a_valid_api_key";
        try {
            signInCtrl( apiKey );
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        ProfileGenerator profileGenerator = ProfileGenerator.getProfileGenerator();
        SeedsConfig seedsConfig = new SeedsConfig.SeedsBuilder()
                .addRange( 0, 60 )
                .addRange( 70, 1000 )
                .build();

        try {
            Profile profile = profileGenerator.createProfile( apiKey, seedsConfig );

            String orderKey = issueOrder( apiKey, profile );

            refreshCtrlStatus( apiKey, "START");

            Profile profile2 = buildCompositeProfile( orderKey );

            Gson gson = new GsonBuilder().setPrettyPrinting().create();

            log.debug("Created Profile: " + gson.toJson( profile2 ) );

            PolyphenyDbRequest.Result request = new PolyphenyDbRequest.Result( apiKey, orderKey );
            request.seed = 1337L;
            request.resultSetHash = 1291L;
            request.success = true;
            request.error = String.valueOf( new IllegalArgumentException("False is not true.") );
            request.logical = "Solve the equation.";
            request.physical = "Get stuck solving the equation.";
            request.actual = 1000000L;
            request.predicted = 10L;

            insertResult(
                    request.getOrderKey(),
                    request.getSeed(),
                    request.getSuccess(),
                    request.getError(),
                    request.getResultSetHash(),
                    request.getLogical(),
                    request.getPhysical(),
                    request.getActual(),
                    request.getPredicted()
            );


            Profile profile3 = profileGenerator.createProfile( apiKey, seedsConfig );

            String orderKey2 = issueOrder( apiKey, profile3 );

            PolyphenyDbRequest.Result request2 = new PolyphenyDbRequest.Result( apiKey, orderKey2 );
            request2.seed = 1337L;
            request2.resultSetHash = 1291L;
            request2.success = true;
            request2.error = String.valueOf( new IllegalArgumentException("False is not true.") );
            request2.logical = "Solve the equation 2.";
            request2.physical = "Get stuck solving the equation 2.";
            request2.actual = 1000000L;
            request2.predicted = 10L;

            insertResult(
                    request2.getOrderKey(),
                    request2.getSeed(),
                    request2.getSuccess(),
                    request2.getError(),
                    request2.getResultSetHash(),
                    request2.getLogical(),
                    request2.getPhysical(),
                    request2.getActual(),
                    request2.getPredicted()
            );

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void signInCtrl( String apiKey ) throws SQLException {

        if ( ctrlNotRegistered(apiKey) ) {
            log.debug( "Registering at sign in: " + apiKey );
            registerCtrl( apiKey );
            return;
        }

        if ( ctrlIsSignedIn( apiKey ) ) {
            log.debug( "Control Client already signed in: " + apiKey );
            return;
        }

        log.debug("Signing in " + apiKey );
        try ( PreparedStatement preparedStatement = PolySQL.prepare(PolySQL.SIGN_IN_CTRL, apiKey)) {
            preparedStatement.execute();
        }

    }

    private boolean ctrlIsSignedIn( String apiKey ) throws SQLException {
        boolean ctrIsSignedIn;
        try ( PreparedStatement preparedStatement = PolySQL.prepare(PolySQL.IS_SIGNED_IN, apiKey)) {
            ctrIsSignedIn = resultExists( preparedStatement );
        }
        return ctrIsSignedIn;
    }

    private boolean ctrlNotRegistered(String apiKey ) throws SQLException {
        boolean ctrlIsRegistered;
        try ( PreparedStatement preparedStatement = PolySQL.prepare(PolySQL.CTRL_EXISTS, apiKey)) {
            ctrlIsRegistered = resultExists( preparedStatement );
        }
        return !ctrlIsRegistered;
    }

    public void refreshCtrlStatus( String apiKey, String status ) throws SQLException {
        if ( ctrlNotRegistered(apiKey) ) {
            log.debug( "Rediscovered client: " + apiKey + ", with status: " + status );
            registerCtrl( apiKey );
            signInCtrl( apiKey );
        } else {
            log.debug( "Client: " + apiKey + ", reports status: " + status );
        }

        StatusType statusType = Enum.valueOf( StatusType.class, status );
        try (
                PreparedStatement preparedStatement = PolySQL.prepare(
                        PolySQL.REFRESH_CTRL,
                        statusType.get(),
                        new Timestamp( System.currentTimeMillis() ),
                        apiKey
                        )
        ) {
            preparedStatement.execute();
        }
    }

    public void signOutCtrl( String apiKey ) throws SQLException {
        boolean ctrIsSignedIn;
        try ( PreparedStatement preparedStatement = PolySQL.prepare(PolySQL.IS_SIGNED_IN, apiKey)) {
            ctrIsSignedIn = resultExists( preparedStatement );
        }

        if ( ! ctrIsSignedIn ) {
            log.debug( "Control Client already signed out: " + apiKey );
            return;
        }

        log.debug("Signing out: " + apiKey );

        // Todo Sign-Out PolySQL
    }


    public void registerCtrl( String apiKey ) throws SQLException {
        try (
                PreparedStatement preparedStatement = PolySQL.prepare(
                        PolySQL.REGISTER_CTRL,
                        apiKey,
                        Boolean.TRUE,
                        new Timestamp( System.currentTimeMillis() ),
                        StatusType.IDLE.get()
                )
        ) {
            preparedStatement.execute();
        }
    }

    private boolean configDoesNotExists(ConfigType configType, Long hash ) throws SQLException {
        boolean configExists;
        try ( PreparedStatement preparedStatement = PolySQL.prepare( PolySQL.CONFIG_EXISTS, configType, hash )) {
            configExists = resultExists( preparedStatement );
        }
        return !configExists;
    }

    private void insertConfig( ConfigType configType, Long hash, String config ) throws SQLException {
        try ( PreparedStatement preparedStatement = PolySQL.prepare( PolySQL.INSERT_CONFIG, configType, hash, config )) {
            preparedStatement.execute();
        }
    }

    private boolean profileCompositeExists( long hash ) throws SQLException {
        boolean profileCompositeExists;
        try ( PreparedStatement preparedStatement = PolySQL.prepare( PolySQL.PROFILE_COMPOSITE_EXISTS, hash )) {
            profileCompositeExists = resultExists( preparedStatement );
        }
        return profileCompositeExists;
    }

    private void insertProfile( Object...args ) throws SQLException {
        try ( PreparedStatement preparedStatement = PolySQL.prepare(
                PolySQL.INSERT_PROFILE,
                args
            )
        ) {
            preparedStatement.execute();
        }
    }

    /**
     * Issues an order for a profile and saves it to the database if it doesn't already exist. Moreover, this function
     * inserts newly generated configurations which are not already present in the database. If the configurations are
     * a new profile it will also insert a new profile record.
     *
     * @param apiKey the API key associated with the PolyphenyControl Instance the order is issued to.
     * @param profile the profile object containing various configurations for the order.
     * @return orderKey a unique identifier for the issued order.
     * @throws SQLException if any database operation fails during the process.
     */
    public String issueOrder(
            String apiKey,
            Profile profile
    ) throws SQLException {

        LinkedList<Long> hashes = new LinkedList<>();
        boolean profileConfigsExist = true;
        for ( Config config : List.of(
                profile.getSchemaConfig(),
                profile.getDataConfig(),
                profile.getQueryConfig(),
                profile.getStoreConfig(),
                profile.getStartConfig(),
                profile.getPartitionConfig()
        ) ) {
            // Do we have this configuration already saved?
            Pair<Long, String> pair = config.hashAndString();
            if ( configDoesNotExists( config.getType(), pair.getLeft() ) ) {
                // If not we insert it
                insertConfig( config.getType(), pair.getLeft(), pair.getRight() );
                profileConfigsExist = false;
            }
            // Also save the hash for comparing the profile composite.
            hashes.addLast( pair.getLeft() );
        }

        long profileKey = Config.rehash( hashes.toArray( Long[]::new ) );
        if ( ! profileConfigsExist ) {
            // We already have this profile in its parts, do we have it as a composite?
            if ( ! profileCompositeExists( profileKey ) ) {
                // No? In that case we will insert it.
                hashes.addFirst( profileKey );
                insertProfile( hashes.toArray() );
            }
        }

        // Now we issue the associated order...
        String orderKey = UUID.randomUUID().toString();
        try (
                PreparedStatement preparedStatement = PolySQL.prepare(
                        PolySQL.INSERT_ORDER,
                        orderKey,
                        apiKey,
                        profileKey,
                        profile.getIssuedSeeds().hashAndString().getRight(),
                        new Timestamp( System.currentTimeMillis() ),
                        null
                )

        ) {
            preparedStatement.execute();
        }

        // Returning the orderKey for response to ctrl client
        return orderKey;

    }

    /**
     * This function is responsible for building a composite profile from an order key.
     * It fetches the necessary configurations from the database and constructs a Profile.
     *
     * @param orderKey the unique identifier of the order.
     * @return a Profile object containing all the configurations for the order.
     * @throws SQLException if any database operation fails during the process.
     * @throws RuntimeException if any order or any configuration of the order is not present in the database.
     */
    public Profile buildCompositeProfile( String orderKey ) throws SQLException {

        String apiKey;
        long schemaConfigHash;
        long dataConfigHash;
        long queryConfigHash;
        long storeConfigHash;
        long startConfigHash;
        long partConfigHash;
        String seeds;

        try ( PreparedStatement preparedStatement = PolySQL.prepare( PolySQL.COMPOSITE_JOB1, orderKey )) {
            preparedStatement.execute();
            ResultSet resultSet = preparedStatement.getResultSet();

            if ( ! resultSet.next() ) {
                // Should not happen: There is no profile for the order.
                throw new RuntimeException("Could not build composite Profile");
            }

            apiKey = resultSet.getString( 1 );
            schemaConfigHash = resultSet.getLong( 2 );
            dataConfigHash = resultSet.getLong( 3 );
            queryConfigHash = resultSet.getLong( 4 );
            storeConfigHash = resultSet.getLong( 5 );
            startConfigHash = resultSet.getLong( 6 );
            partConfigHash = resultSet.getLong( 7 );
            seeds = resultSet.getString( 8 );

            resultSet.close();
        }

        Profile profile;

        try ( PreparedStatement preparedStatement = PolySQL.prepare(
                PolySQL.COMPOSITE_JOB2,
                schemaConfigHash,
                dataConfigHash,
                queryConfigHash,
                storeConfigHash,
                startConfigHash,
                partConfigHash
            )
        ) {
            preparedStatement.execute();
            ResultSet resultSet = preparedStatement.getResultSet();

            if ( ! resultSet.next() ) {
                // Should not happen: There is no profile for the order.
                throw new RuntimeException("Could not build composite Profile");
            }

            Gson gson = new Gson();

            SchemaConfig schemaConfig = gson.fromJson( resultSet.getString(1 ), SchemaConfig.class );
            DataConfig dataConfig = gson.fromJson( resultSet.getString(2 ), DataConfig.class );
            QueryConfig queryConfig = gson.fromJson( resultSet.getString(3 ), QueryConfig.class );
            StoreConfig storeConfig = gson.fromJson( resultSet.getString(4 ), StoreConfig.class );
            StartConfig startConfig = gson.fromJson( resultSet.getString(5 ), StartConfig.class );
            PartitionConfig partConfig = gson.fromJson( resultSet.getString(6 ), PartitionConfig.class );

            resultSet.close();

            SeedsConfig seedsConfig = gson.fromJson( seeds, SeedsConfig.class );

            profile = Profile.builder()
                    .apiKey( apiKey )
                    .schemaConfig( schemaConfig )
                    .dataConfig( dataConfig )
                    .queryConfig( queryConfig )
                    .storeConfig( storeConfig )
                    .startConfig( startConfig )
                    .partitionConfig( partConfig )
                    .createdAt( null )
                    .completedAt( null )
                    .issuedSeeds( seedsConfig )
                    .build();
        }

        return profile;

    }

    private boolean resultExists( PreparedStatement preparedStatement ) throws SQLException {
        preparedStatement.execute();
        ResultSet resultSet = preparedStatement.getResultSet();
        return resultSet.next();
    }

    public void insertResult( String orderKey, long seed, boolean success, String error, Long resultSetHash, String logicalPlan, String physicalPlan, Long actualMs, Long predictedMs ) throws SQLException {
        ErrorConfig errorConfig = new ErrorConfig( error );
        PhysicalPlanConfig physicalPlanConfig = new PhysicalPlanConfig( physicalPlan );
        LogicalPlanConfig logicalPlanConfig = new LogicalPlanConfig( logicalPlan );

        Pair<Long, String> errorPair = errorConfig.hashAndString();
        Pair<Long, String> physicalPlanPair = physicalPlanConfig.hashAndString();
        Pair<Long, String> logicalPlanPair = logicalPlanConfig.hashAndString();

        if (configDoesNotExists(ConfigType.ERROR, errorPair.getLeft())) {
            insertConfig( ConfigType.ERROR, errorPair.getLeft(), errorPair.getRight() );
        }

        if (configDoesNotExists(ConfigType.PHYSICAL_PLAN, physicalPlanPair.getLeft())) {
            insertConfig( ConfigType.PHYSICAL_PLAN, physicalPlanPair.getLeft(), physicalPlanPair.getRight() );
        }

        if (configDoesNotExists(ConfigType.LOGICAL_PLAN, logicalPlanPair.getLeft())) {
            insertConfig( ConfigType.LOGICAL_PLAN, logicalPlanPair.getLeft(), logicalPlanPair.getRight() );
        }

        try (
                PreparedStatement preparedStatement = PolySQL.prepare(
                        PolySQL.INSERT_RESULT,
                        UUID.randomUUID().toString(),
                        orderKey,
                        seed,
                        success,
                        new Timestamp( System.currentTimeMillis() ),
                        errorPair.getLeft(),
                        resultSetHash,
                        logicalPlanPair.getLeft(),
                        physicalPlanPair.getLeft(),
                        actualMs,
                        predictedMs
                )

        ) {
            preparedStatement.execute();
        }

    }


    public boolean isActive() {
        try {
            return this.connection.isValid( 5000 );
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
