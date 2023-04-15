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

import lombok.AllArgsConstructor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

@AllArgsConstructor
public enum PolySQL {
//    /**
//     * <pre>
//     * UPDATE polyfier.nodes SET status = ?, last_seen = ?
//     * WHERE (
//     *      api_key = ?
//     * )
//     * </pre>
//     */
//    REFRESH_CTRL("""
//                    UPDATE polyfier.nodes SET status = ?, last_seen = ? WHERE (
//                        api_key = ?
//                    )
//            """),
//    /**
//     * <pre>
//     * UPDATE polyfier.nodes SET logged_in = FALSE, status = 'UNKNOWN'
//     * WHERE (
//     *      last_seen < ?
//     * )
//     * </pre>
//     */
//    SIGN_OUT_LOST("""
//                    UPDATE polyfier.nodes SET logged_in = FALSE, status = 'UNKNOWN' WHERE (
//                        last_seen < ?
//                    )
//            """),
    /**
     * <pre>
     * INSERT INTO polyfier.pctrl (
     *      pctrlKey, branch, registeredAt
     * ) VALUES (
     *      ?, ?, ?
     * )
     * </pre>
     */
    REGISTER_PCTRL("""
                    INSERT INTO polyfier.pctrl (
                        pctrlKey, branch, registeredAt
                    ) VALUES (
                        ?, ?, ?
                    )
            """),
    /**
     * <pre>
     * UPDATE polyfier.pctrl SET active = TRUE WHERE (
     *      pctrlKey = ?
     * )
     * </pre>
     */
    SIGN_IN_PCTRL("""
                    UPDATE polyfier.pctrl SET active = TRUE WHERE (
                        pctrlKey = ?
                    )
            """),
    /**
     * <pre>
     * SELECT * FROM polyfier.pctrl WHERE (
     *  pctrlKey = ?
     * )
     * </pre>
     */
    GET_PCTRL("""
                    SELECT * FROM polyfier.pctrl WHERE (
                        pctrlKey = ?
                    )
            """),
//    /**
//     * <pre>
//     * SELECT * FROM polyfier.pctrl WHERE (
//     *      pctrlKey = ? AND
//     *      active = TRUE
//     * )
//     * </pre>
//     */
//    GET_PCTRL_IF_ACTIVE("""
//                    SELECT * FROM polyfier.pctrl WHERE (
//                        pctrlKey = ? AND
//                        active = TRUE
//                    )
//            """),
    /**
     * <pre>
     * INSERT INTO polyfier.#SIGNATURE_configs (
     *      #SIGNATURE_config_hash, #SIGNATURE_config
     * ) VALUES (
     *      ?, ?
     * )
     * </pre>
     */
    INSERT_CONFIG("""
                INSERT INTO polyfier.%s_configs (
                    %s_config_hash, %s_config
                ) VALUES (
                    ?, ?
                )
            """),
    /**
     * <pre>
     * INSERT INTO polyfier.pdb (
     *      pdbKey, branch, registeredAt, pctrl_key, schema_config_hash, data_config_hash, query_config_hash, store_config_hash, start_config_hash, part_config_hash
     * ) VALUES (
     *      ?, ?, ?, ?, ?, ?, ?, ?, ?
     * )
     * </pre>
     */
    REGISTER_PDB("""
                INSERT INTO polyfier.pdb (
                    pdbKey, branch, registeredAt, pctrlKey, schema_config_hash, data_config_hash, query_config_hash, store_config_hash, start_config_hash, part_config_hash
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
            """),
//    /**
//     * <pre>
//     * INSERT INTO polyfier.orders (
//     *      order_key, api_key, profile_key, seeds, issued_at, completed_at
//     * ) VALUES (
//     *      ?, ?, ?, ?, ?, ?
//     * )
//     * </pre>
//     */
//    INSERT_ORDER("""
//                    INSERT INTO polyfier.orders (
//                        order_key, api_key, profile_key, seeds, issued_at, completed_at
//                    ) VALUES (
//                        ?, ?, ?, ?, ?, ?
//                    )
//            """),
    /**
     * <pre>
     * SELECT *
     * FROM polyfier.#SIGNATURE_configs
     * WHERE (
     *     #SIGNATURE_config_hash = ?
     * )
     * </pre>
     */
    CONFIG_EXISTS("""
                    SELECT * FROM polyfier.%s_configs WHERE (
                        %s_config_hash = ?
                    )
            """),
//    /**
//     * <pre>
//     * SELECT * FROM polyfier.profiles
//     * WHERE (
//     *      profiles.profile_key = ?
//     * )
//     * </pre>
//     */
//    PROFILE_COMPOSITE_EXISTS("""
//                SELECT * FROM polyfier.profiles WHERE (
//                    profiles.profile_key = ?
//                )
//            """),
//    /**
//     * <pre>
//     * SELECT * FROM polyfier.nodes
//     * WHERE (
//     *      api_key = ? AND
//     *      status = 'IDLE'
//     * )
//     * </pre>
//     */
//    CTRL_IS_IDLE("""
//                SELECT * FROM polyfier.nodes WHERE (
//                    api_key = ? AND
//                    status = 'IDLE'
//                )
//            """),
    /**
     * <pre>
     * SELECT * FROM polyfier.orders
     * WHERE (
     *      orderId = ?
     * )
     * </pre>
     */
    GET_PDB("""
                SELECT * FROM polyfier.pdb WHERE (
                    pdbKey = ?
                )
            """),
//    /**
//     * SELECT orders.api_key, profiles.schema_config_hash, profiles.data_config_hash, profiles.query_config_hash, profiles.store_config_hash, profiles.start_config_hash, profiles.part_config_hash, orders.seeds
//     * FROM polyfier.orders, polyfier.profiles WHERE (
//     *     orders.order_key = ? AND
//     *     orders.profile_key = profiles.profile_key
//     * )
//     */
//    COMPOSITE_JOB1("""
//                SELECT orders.api_key, profiles.schema_config_hash, profiles.data_config_hash, profiles.query_config_hash, profiles.store_config_hash, profiles.start_config_hash, profiles.part_config_hash, orders.seeds
//                FROM polyfier.orders, polyfier.profiles WHERE (
//                    orders.order_key = ? AND
//                    orders.profile_key = profiles.profile_key
//                )
//            """),
//    /**
//     * SELECT schema_configs.schema_config, query_configs.query_config, data_configs.data_config, store_configs.store_config, start_configs.start_config, part_configs.part_config
//     * FROM polyfier.schema_configs,  polyfier.data_configs,  polyfier.query_configs,  polyfier.store_configs,  polyfier.start_configs, polyfier.part_configs
//     * WHERE (
//     * 	schema_configs.schema_config_hash = ? AND
//     * 	data_configs.data_config_hash = ? AND
//     * 	query_configs.query_config_hash = ? AND
//     * 	store_configs.store_config_hash = ? AND
//     * 	start_configs.start_config_hash = ? AND
//     * 	part_configs.part_config_hash = ?
//     * )
//     */
//    COMPOSITE_JOB2("""
//                SELECT schema_configs.schema_config, query_configs.query_config, data_configs.data_config, store_configs.store_config, start_configs.start_config, part_configs.part_config
//                FROM polyfier.schema_configs,  polyfier.data_configs,  polyfier.query_configs,  polyfier.store_configs,  polyfier.start_configs, polyfier.part_configs
//                WHERE (
//                	schema_configs.schema_config_hash = ? AND
//                	data_configs.data_config_hash = ? AND
//                	query_configs.query_config_hash = ? AND
//                	store_configs.store_config_hash = ? AND
//                	start_configs.start_config_hash = ? AND
//                	part_configs.part_config_hash = ?
//                )
//            """),
    /**
     * <pre>
     * INSERT INTO polyfier.%s_configs (
     *      %s_config_hash, %s_config
     * ) VALUES (
     *      ?, ?
     * )
     * </pre>
     */
    INSERT_RESULT_CONFIGS("""
                INSERT INTO polyfier.%s_configs (
                    %s_config_hash, %s_config
                ) VALUES (
                    ?, ?
                )
            """),
    /**
     * <pre>
     * INSERT INTO polyfier.results (
     * resultKey#1, pdbKey#2, seed#3, success#4, receivedAt#5, errorHash#6, resultSetHash#7, logicalHash#8, physicalHash#9, execTime#10
     * ) VALUES (
     * #1, #2, #3, #4, #5, #6, #7, #8, #9, #10
     * )
     * </pre>
     */
    INSERT_RESULT("""
                INSERT INTO polyfier.results (
                    resultKey, pdbKey, seed, success, receivedAt, errorHash, resultSetHash, logicalHash, physicalHash, execTime
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
            """);

    private final String polySql;

    private String get() {
        return polySql;
    }

    public static PreparedStatement prepare( Connection connection, PolySQL polySQL, Object... args ) throws SQLException {
        PreparedStatement statement;
        switch ( polySQL ) {
//            case REFRESH_CTRL -> {
//                PreparedStatement preparedStatement = connection.prepareStatement( polySQL.get() );
//                preparedStatement.setString( 1, (String) args[0]);
//                preparedStatement.setTimestamp( 2, (Timestamp) args[1] );
//                preparedStatement.setString( 3, (String) args[2]);
//                statement = preparedStatement;
//            }
//            case SIGN_OUT_LOST -> {
//                PreparedStatement preparedStatement = connection.prepareStatement( polySQL.get() );
//                preparedStatement.setTimestamp( 1, (Timestamp) args[0]);
//                statement = preparedStatement;
//            }
            case REGISTER_PCTRL -> {
                PreparedStatement preparedStatement = connection.prepareStatement( polySQL.get() );
                preparedStatement.setString( 1, (String) args[0]);
                preparedStatement.setString( 2, (String) args[1] );
                preparedStatement.setTimestamp( 3, (Timestamp) args[2]);
                statement = preparedStatement;
            }
            case SIGN_IN_PCTRL, GET_PCTRL, GET_PDB -> {
                PreparedStatement preparedStatement = connection.prepareStatement( polySQL.get() );
                preparedStatement.setString( 1, (String) args[0]);
                statement = preparedStatement;
            }
            case INSERT_CONFIG, INSERT_RESULT_CONFIGS -> {
                String signature = ( (ConfigType) args[0] ).getSignature();
                PreparedStatement preparedStatement = connection.prepareStatement(
                        String.format(
                                polySQL.get(),
                                signature,
                                signature,
                                signature
                        )
                );
                preparedStatement.setLong( 1, (Long) args[1]);
                preparedStatement.setString( 2, (String) args[2]);
                statement = preparedStatement;
            }
            case REGISTER_PDB -> {
                PreparedStatement preparedStatement = connection.prepareStatement( polySQL.get() );
                preparedStatement.setString( 1, (String) args[0]);
                preparedStatement.setString( 2, (String) args[1]);
                preparedStatement.setTimestamp( 3, (Timestamp) args[2]);
                preparedStatement.setString( 4, (String) args[3]);
                preparedStatement.setLong( 5, (Long) args[4]);
                preparedStatement.setLong( 6, (Long) args[5]);
                preparedStatement.setLong( 7, (Long) args[6]);
                preparedStatement.setLong( 8, (Long) args[7]);
                preparedStatement.setLong( 9, (Long) args[8]);
                preparedStatement.setLong( 10, (Long) args[9]);
                statement = preparedStatement;
            }
//            case INSERT_ORDER -> {
//                PreparedStatement preparedStatement = connection.prepareStatement( polySQL.get() );
//                preparedStatement.setString( 1, (String) args[0]);
//                preparedStatement.setString( 2, (String) args[1]);
//                preparedStatement.setLong( 3, (Long) args[2]);
//                preparedStatement.setString( 4, (String) args[3]);
//                preparedStatement.setTimestamp( 5, (Timestamp) args[4]);
//                preparedStatement.setTimestamp( 6, (Timestamp) args[5]);
//                statement = preparedStatement;
//            }
            case CONFIG_EXISTS -> {
                String signature = ( (ConfigType) args[0] ).getSignature();
                PreparedStatement preparedStatement = connection.prepareStatement(
                        String.format(
                                polySQL.get(),
                                signature,
                                signature
                        )
                );
                preparedStatement.setLong( 1, (Long) args[1]);
                statement = preparedStatement;
            }
//            case PROFILE_COMPOSITE_EXISTS -> {
//                PreparedStatement preparedStatement = connection.prepareStatement( polySQL.get() );
//                preparedStatement.setLong( 1, (Long) args[0]);
//                statement = preparedStatement;
//            }
            case INSERT_RESULT -> {
                PreparedStatement preparedStatement = connection.prepareStatement( polySQL.get() );
                preparedStatement.setString( 1, (String) args[0]);
                preparedStatement.setString( 2, (String) args[1]);
                preparedStatement.setLong( 3, (Long) args[2]);
                preparedStatement.setBoolean( 4, (Boolean) args[3] );
                preparedStatement.setTimestamp( 5, (Timestamp) args[4]);
                preparedStatement.setLong( 6, (Long) args[5]);
                preparedStatement.setLong( 7, (Long) args[6]);
                preparedStatement.setLong( 8, (Long) args[7]);
                preparedStatement.setLong( 9, (Long) args[8]);
                preparedStatement.setLong( 10, (Long) args[9]);
                statement = preparedStatement;
            }
//            case COMPOSITE_JOB2 -> {
//                PreparedStatement preparedStatement = connection.prepareStatement( polySQL.get() );
//                preparedStatement.setLong( 1, (Long) args[0]);
//                preparedStatement.setLong( 2, (Long) args[1]);
//                preparedStatement.setLong( 3, (Long) args[2]);
//                preparedStatement.setLong( 4, (Long) args[3]);
//                preparedStatement.setLong( 5, (Long) args[4]);
//                preparedStatement.setLong( 6, (Long) args[5]);
//                statement = preparedStatement;
//            }
            default -> {
                throw new IllegalArgumentException("Invalid PolySQL Enum Constant.");
            }
        }

        return statement;
    }

}
