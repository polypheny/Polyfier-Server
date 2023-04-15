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

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import server.clients.PCtrl;
import server.clients.PDB;
import server.profile.*;
import server.profile.Profile;
import server.messages.ClientMessage;

import java.sql.*;
import java.util.UUID;

@Slf4j
@Getter(AccessLevel.PRIVATE)
@AllArgsConstructor
public class QueryLogConnection {
    @Getter
    private Connection connection;

    public static void initialize( String url, String user, String password ) {
        new QueryLogAdapter().setUp( url, user, password );
    }

    public static QueryLogConnection with( String url, String user, String password ) throws SQLException {
        Connection connection = new QueryLogAdapter().connect( url, user, password ).orElseThrow();
        return new QueryLogConnection( connection );
    }

    public void close() throws SQLException {
        this.connection.commit();
        this.connection.close();
    }

    synchronized public void registerPctrl( PCtrl pCtrl ) throws SQLException {
        if ( log.isDebugEnabled() ) {
            log.debug( "Registering Pctrl: " + pCtrl.getPctrlKey() );
        }
        try ( PreparedStatement preparedStatement = PolySQL.prepare(
                getConnection(),
                PolySQL.REGISTER_PCTRL,
                pCtrl.getPctrlKey(),
                pCtrl.getBranch(),
                new Timestamp( pCtrl.getRegisteredAt() )
        )) {
            preparedStatement.execute();
        }
    }

//    private boolean ctrlIsSignedIn( String apiKey ) throws SQLException {
//        boolean ctrIsSignedIn;
//        try ( PreparedStatement preparedStatement = PolySQL.prepare( getConnection(), PolySQL.IS_SIGNED_IN, apiKey)) {
//            ctrIsSignedIn = resultExists( preparedStatement );
//        }
//        return ctrIsSignedIn;
//    }

//    private boolean ctrlNotRegistered( String apiKey ) throws SQLException {
//        boolean ctrlIsRegistered;
//        try ( PreparedStatement preparedStatement = PolySQL.prepare( getConnection(), PolySQL.GET_PCTRL, apiKey)) {
//            ctrlIsRegistered = resultExists( preparedStatement );
//        }
//        return !ctrlIsRegistered;
//    }

//    public void refreshCtrlStatus( String apiKey, String status ) throws SQLException {
//        if ( ctrlNotRegistered(apiKey) ) {
//            log.debug( "Rediscovered client: " + apiKey + ", with status: " + status );
//            registerCtrl( apiKey );
//            singInPctrl( apiKey );
//        } else {
//            log.debug( "Client: " + apiKey + ", reports status: " + status );
//        }
//
//        try {
//            StatusType statusType = Enum.valueOf( StatusType.class, status );
//            try (
//                    PreparedStatement preparedStatement = PolySQL.prepare(
//                            getConnection(),
//                            PolySQL.REFRESH_CTRL,
//                            statusType.get(),
//                            new Timestamp( System.currentTimeMillis() ),
//                            apiKey
//                    )
//            ) {
//                preparedStatement.execute();
//            }
//        } catch ( Exception exception ) {
//            log.error( "Error: ", exception );
//            throw new RuntimeException(exception);
//        }
//
//    }

//    public void signOutCtrl( String apiKey ) throws SQLException {
//        boolean ctrIsSignedIn;
//        try ( PreparedStatement preparedStatement = PolySQL.prepare( getConnection(), PolySQL.IS_SIGNED_IN, apiKey)) {
//            ctrIsSignedIn = resultExists( preparedStatement );
//        }
//
//        if ( ! ctrIsSignedIn ) {
//            log.debug( "Control Client already signed out: " + apiKey );
//            return;
//        }
//
//        log.debug("Signing out: " + apiKey );
//
//        // Todo Sign-Out PolySQL
//    }


//    public void registerCtrl( String apiKey ) throws SQLException {
//        try (
//                PreparedStatement preparedStatement = PolySQL.prepare(
//                        getConnection(),
//                        PolySQL.REGISTER_PCTRL,
//                        apiKey,
//                        Boolean.TRUE,
//                        new Timestamp( System.currentTimeMillis() ),
//                        StatusType.IDLE.get()
//                )
//        ) {
//            preparedStatement.execute();
//        }
//    }

    private boolean configDoesNotExists(ConfigType configType, Long hash ) throws SQLException {
        boolean configExists;
        try ( PreparedStatement preparedStatement = PolySQL.prepare( getConnection(), PolySQL.CONFIG_EXISTS, configType, hash )) {
            configExists = resultExists( preparedStatement );
        }
        return !configExists;
    }

    private void insertConfig( ConfigType configType, Long hash, String config ) throws SQLException {
        try ( PreparedStatement preparedStatement = PolySQL.prepare( getConnection(), PolySQL.INSERT_CONFIG, configType, hash, config )) {
            preparedStatement.execute();
        }
    }

//    private boolean profileCompositeExists( long hash ) throws SQLException {
//        boolean profileCompositeExists;
//        try ( PreparedStatement preparedStatement = PolySQL.prepare( getConnection(), PolySQL.PROFILE_COMPOSITE_EXISTS, hash )) {
//            profileCompositeExists = resultExists( preparedStatement );
//        }
//        return profileCompositeExists;
//    }

    synchronized public void registerPdb( PDB pdb, Profile profile ) throws SQLException {
        if ( log.isDebugEnabled() ) {
            log.debug( "Registering PDB: " + pdb.getPdbKey() + " associated with Pctrl" + pdb.getPctrlKey() );
        }

        Pair<Long, String> schemaConfig = profile.getSchemaConfig().hashAndString();
        if ( configDoesNotExists( ConfigType.SCHEMA, schemaConfig.getLeft() ) ) {
            insertConfig( ConfigType.SCHEMA, schemaConfig.getLeft(), schemaConfig.getRight() );
        }

        Pair<Long, String> dataConfig = profile.getDataConfig().hashAndString();
        if ( configDoesNotExists( ConfigType.DATA, dataConfig.getLeft() ) ) {
            insertConfig( ConfigType.DATA, dataConfig.getLeft(), dataConfig.getRight() );
        }

        Pair<Long, String> queryConfig = profile.getQueryConfig().hashAndString();
        if ( configDoesNotExists( ConfigType.QUERY, queryConfig.getLeft() ) ) {
            insertConfig( ConfigType.QUERY, queryConfig.getLeft(), queryConfig.getRight() );
        }

        Pair<Long, String> storeConfig = profile.getStoreConfig().hashAndString();
        if ( configDoesNotExists( ConfigType.STORE, storeConfig.getLeft() ) ) {
            insertConfig( ConfigType.STORE, storeConfig.getLeft(), storeConfig.getRight() );
        }

        Pair<Long, String> startConfig = profile.getStartConfig().hashAndString();
        if ( configDoesNotExists( ConfigType.START, startConfig.getLeft() ) ) {
            insertConfig( ConfigType.START, startConfig.getLeft(), startConfig.getRight() );
        }

        Pair<Long, String> partitionConfig = profile.getPartitionConfig().hashAndString();
        if ( configDoesNotExists( ConfigType.PART, partitionConfig.getLeft() ) ) {
            insertConfig( ConfigType.PART, partitionConfig.getLeft(), partitionConfig.getRight() );
        }

        try ( PreparedStatement preparedStatement = PolySQL.prepare(
                getConnection(),
                PolySQL.REGISTER_PDB,
                pdb.getPdbKey(),
                pdb.getBranch(),
                new Timestamp( pdb.getRegisteredAt() ),
                pdb.getPctrlKey(),
                schemaConfig.getLeft(),
                dataConfig.getLeft(),
                queryConfig.getLeft(),
                storeConfig.getLeft(),
                startConfig.getLeft(),
                partitionConfig.getLeft()

        )) {
            preparedStatement.execute();
        }
    }

//    /**
//     * Issues an order for a profile and saves it to the database if it doesn't already exist. Moreover, this function
//     * inserts newly generated configurations which are not already present in the database. If the configurations are
//     * a new profile it will also insert a new profile record.
//     *
//     * @param apiKey the API key associated with the PolyphenyControl Instance the order is issued to.
//     * @param profile the profile object containing various configurations for the order.
//     * @return orderKey a unique identifier for the issued order.
//     * @throws SQLException if any database operation fails during the process.
//     */
//    public String issueOrder(
//            String apiKey,
//            Profile profile
//    ) throws SQLException {
//
//        LinkedList<Long> hashes = new LinkedList<>();
//        boolean profileConfigsExist = true;
//        for ( Config config : List.of(
//                profile.getSchemaConfig(),
//                profile.getDataConfig(),
//                profile.getQueryConfig(),
//                profile.getStoreConfig(),
//                profile.getStartConfig(),
//                profile.getPartitionConfig()
//        ) ) {
//            // Do we have this configuration already saved?
//            Pair<Long, String> pair = config.hashAndString();
//            if ( configDoesNotExists( config.getType(), pair.getLeft() ) ) {
//                // If not we insert it
//                insertConfig( config.getType(), pair.getLeft(), pair.getRight() );
//                profileConfigsExist = false;
//            }
//            // Also save the hash for comparing the profile composite.
//            hashes.addLast( pair.getLeft() );
//        }
//
//        long profileKey = Config.rehash( hashes.toArray( Long[]::new ) );
//        if ( ! profileConfigsExist ) {
//            // We already have this profile in its parts, do we have it as a composite?
//            if ( ! profileCompositeExists( profileKey ) ) {
//                // No? In that case we will insert it.
//                hashes.addFirst( profileKey );
//                insertProfile( hashes.toArray() );
//            }
//        }
//
//        // Now we issue the associated order...
//        String orderKey = UUID.randomUUID().toString();
//        try (
//                PreparedStatement preparedStatement = PolySQL.prepare(
//                        getConnection(),
//                        PolySQL.INSERT_ORDER,
//                        orderKey,
//                        apiKey,
//                        profileKey,
//                        profile.getIssuedSeeds().hashAndString().getRight(),
//                        new Timestamp( System.currentTimeMillis() ),
//                        null
//                )
//
//        ) {
//            preparedStatement.execute();
//        }
//
//        // Returning the orderKey for response to ctrl client
//        return orderKey;
//
//    }

//    /**
//     * This function is responsible for building a composite profile from an order key.
//     * It fetches the necessary configurations from the database and constructs a Profile.
//     *
//     * @param orderKey the unique identifier of the order.
//     * @return a Profile object containing all the configurations for the order.
//     * @throws SQLException if any database operation fails during the process.
//     * @throws RuntimeException if any order or any configuration of the order is not present in the database.
//     */
//    public Profile buildCompositeProfile( String orderKey ) throws SQLException {
//
//        String apiKey;
//        long schemaConfigHash;
//        long dataConfigHash;
//        long queryConfigHash;
//        long storeConfigHash;
//        long startConfigHash;
//        long partConfigHash;
//        String seeds;
//
//        try ( PreparedStatement preparedStatement = PolySQL.prepare( getConnection(), PolySQL.COMPOSITE_JOB1, orderKey )) {
//            preparedStatement.execute();
//            ResultSet resultSet = preparedStatement.getResultSet();
//
//            if ( ! resultSet.next() ) {
//                // Should not happen: There is no profile for the order.
//                throw new RuntimeException("Could not build composite Profile");
//            }
//
//            apiKey = resultSet.getString( 1 );
//            schemaConfigHash = resultSet.getLong( 2 );
//            dataConfigHash = resultSet.getLong( 3 );
//            queryConfigHash = resultSet.getLong( 4 );
//            storeConfigHash = resultSet.getLong( 5 );
//            startConfigHash = resultSet.getLong( 6 );
//            partConfigHash = resultSet.getLong( 7 );
//            seeds = resultSet.getString( 8 );
//
//            resultSet.close();
//        }
//
//        Profile profile;
//
//        try ( PreparedStatement preparedStatement = PolySQL.prepare(
//                getConnection(),
//                PolySQL.COMPOSITE_JOB2,
//                schemaConfigHash,
//                dataConfigHash,
//                queryConfigHash,
//                storeConfigHash,
//                startConfigHash,
//                partConfigHash
//            )
//        ) {
//            preparedStatement.execute();
//            ResultSet resultSet = preparedStatement.getResultSet();
//
//            if ( ! resultSet.next() ) {
//                // Should not happen: There is no profile for the order.
//                throw new RuntimeException("Could not build composite Profile");
//            }
//
//            Gson gson = new Gson();
//
//            SchemaConfig schemaConfig = gson.fromJson( resultSet.getString(1 ), SchemaConfig.class );
//            DataConfig dataConfig = gson.fromJson( resultSet.getString(2 ), DataConfig.class );
//            QueryConfig queryConfig = gson.fromJson( resultSet.getString(3 ), QueryConfig.class );
//            StoreConfig storeConfig = gson.fromJson( resultSet.getString(4 ), StoreConfig.class );
//            StartConfig startConfig = gson.fromJson( resultSet.getString(5 ), StartConfig.class );
//            PartitionConfig partConfig = gson.fromJson( resultSet.getString(6 ), PartitionConfig.class );
//
//            resultSet.close();
//
//            SeedsConfig seedsConfig = gson.fromJson( seeds, SeedsConfig.class );
//
//            profile = Profile.builder()
//                    .schemaConfig( schemaConfig )
//                    .dataConfig( dataConfig )
//                    .queryConfig( queryConfig )
//                    .storeConfig( storeConfig )
//                    .startConfig( startConfig )
//                    .partitionConfig( partConfig )
//                    .issuedSeeds( seedsConfig )
//                    .build();
//        }
//
//        return profile;
//
//    }

    private boolean resultExists( PreparedStatement preparedStatement ) throws SQLException {
        preparedStatement.execute();
        ResultSet resultSet = preparedStatement.getResultSet();
        boolean hasNext = resultSet.next();
        resultSet.close();
        return hasNext;
    }

    public void insertResult( PDB pdb, ClientMessage.PDBResult pdbResult ) throws SQLException {

        Long errorHash = null;
        if ( pdbResult.getError() != null ) {
            ErrorConfig errorConfig = new ErrorConfig( pdbResult.getError() );
            Pair<Long, String> errorPair = errorConfig.hashAndString();
            if ( configDoesNotExists(ConfigType.ERROR, errorPair.getLeft()) ) {
                insertConfig( ConfigType.ERROR, errorPair.getLeft(), errorPair.getRight() );
            }
            errorHash = errorPair.getLeft();
        }


        Long physicalPlanHash = null;
        if ( pdbResult.getPhysical() != null ) {
            PhysicalPlanConfig physicalPlanConfig = new PhysicalPlanConfig( pdbResult.getPhysical() );
            Pair<Long, String> physicalPlanPair = physicalPlanConfig.hashAndString();
            if ( configDoesNotExists(ConfigType.PHYSICAL_PLAN, physicalPlanPair.getLeft()) ) {
                insertConfig( ConfigType.PHYSICAL_PLAN, physicalPlanPair.getLeft(), physicalPlanPair.getRight() );
            }
            physicalPlanHash = physicalPlanPair.getLeft();
        }

        Long logicalPlanHash = null;
        if ( pdbResult.getLogical() != null ) {
            LogicalPlanConfig logicalPlanConfig = new LogicalPlanConfig( pdbResult.getLogical() );
            Pair<Long, String> logicalPlanPair = logicalPlanConfig.hashAndString();
            if ( configDoesNotExists(ConfigType.LOGICAL_PLAN, logicalPlanPair.getLeft()) ) {
                insertConfig( ConfigType.LOGICAL_PLAN, logicalPlanPair.getLeft(), logicalPlanPair.getRight() );
            }
            logicalPlanHash = logicalPlanPair.getLeft();
        }

        try (
                PreparedStatement preparedStatement = PolySQL.prepare(
                        getConnection(),
                        PolySQL.INSERT_RESULT,
                        UUID.randomUUID().toString(),
                        pdb.getPdbKey(),
                        pdbResult.getSeed(),
                        pdbResult.getSuccess(),
                        new Timestamp( pdb.getUpdateTime() ),
                        errorHash,
                        pdbResult.getResultSetHash(),
                        logicalPlanHash,
                        physicalPlanHash,
                        pdbResult.getActual()
                )

        ) {
            preparedStatement.execute();
        }

    }


//    public boolean isActive() {
//        try {
//            return this.connection.isValid( 5000 );
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
//    }

}
