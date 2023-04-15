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

package server.profile.generators;

import connect.ConfigType;
import server.profile.SeedsConfig;
import lombok.Builder;
import server.profile.*;

import java.io.Serializable;
import java.util.*;
import java.util.Map;

/**
 * ProfileGenerator is a class responsible for generating different types of
 * configurations and profiles for the test-database system.
 */
@Builder
public class ProfileGenerator implements Serializable {

    private transient Permuter dataConfigPermuter;
    private List<String> dataConfigPermeable;
    private Map<String, String> dataConfigPreset;

    private transient Permuter queryConfigPermuter;
    private List<String> queryConfigPermeable;
    private Map<String, String> queryConfigPreset;


    private transient Permuter storeConfigPermuter;
    private List<String> storeConfigPermeable;
    private Map<String, String> storeConfigPreset;


    private transient Permuter partitionConfigPermuter;
    private List<String> partitionConfigPermeable;
    private Map<String, String> partitionConfigPreset;

    private transient Permuter startConfigPermuter;
    private List<String> startConfigPermeable;
    private Map<String, String> startConfigPreset;

    private SchemaConfig schemaConfig;

    private boolean loopBack; // Else Throw

    private long counter;

    /**
     * Creates a new instance of the ProfileGenerator with the given configuration parameters.
     *
     * @param dataConfigPermeable A list of data configuration keys.
     * @param dataConfigPreset A map of preset data configuration key-value pairs.
     * @param queryConfigPermeable A list of query configuration keys.
     * @param queryConfigPreset A map of preset query configuration key-value pairs.
     * @param storeConfigPermeable A list of store configuration keys.
     * @param storeConfigPreset A map of preset store configuration key-value pairs.
     * @param partitionConfigPermeable A list of partition configuration keys.
     * @param partitionConfigPreset A map of preset partition configuration key-value pairs.
     * @param startConfigPermeable A list of start configuration keys.
     * @param startConfigPreset A map of preset start configuration key-value pairs.
     * @param loopBack A flag indicating whether to loop back when the permutation is finished.
     * @return A new instance of the ProfileGenerator with the provided configuration.
     */
    public static ProfileGenerator create(
            List<String> dataConfigPermeable,
            Map<String, String> dataConfigPreset,
            List<String> queryConfigPermeable,
            Map<String, String> queryConfigPreset,
            List<String> storeConfigPermeable,
            Map<String, String> storeConfigPreset,
            List<String> partitionConfigPermeable,
            Map<String, String> partitionConfigPreset,
            List<String> startConfigPermeable,
            Map<String, String> startConfigPreset,
            boolean loopBack
    ) {
        // Todo Expand
        return ProfileGenerator.builder()
                // Data
                .dataConfigPermeable( dataConfigPermeable )
                .dataConfigPreset( dataConfigPreset )
                .dataConfigPermuter( BooleanPermuter.from( dataConfigPermeable, dataConfigPreset ) )
                // Query
                .queryConfigPermeable( queryConfigPermeable )
                .queryConfigPreset( queryConfigPreset )
                .queryConfigPermuter( BooleanPermuter.from( queryConfigPermeable, queryConfigPreset ) )
                // Store
                .storeConfigPermeable( storeConfigPermeable )
                .storeConfigPreset( storeConfigPreset )
                .storeConfigPermuter( BooleanPermuter.from( storeConfigPermeable, storeConfigPreset ) )
                // Start
                .startConfigPermeable( startConfigPermeable )
                .startConfigPreset( startConfigPreset )
                .startConfigPermuter( BooleanPermuter.from( startConfigPermeable, startConfigPreset ) )
                // Partitions
                .partitionConfigPermeable( partitionConfigPermeable )
                .partitionConfigPreset( partitionConfigPreset )
                .partitionConfigPermuter( BooleanPermuter.from( partitionConfigPermeable, partitionConfigPreset ) )
                // Schema
                .schemaConfig( new SchemaConfig( "default" ) )
                .loopBack( loopBack )
                .counter( 0 )
                .build();
    }

    /**
     * Retrieves the next permutation of configuration parameters from the provided Permuter instance.
     * If the next permutation is not available and the loopBack flag is set, it will loop back to
     * the beginning of the permutation sequence and retrieve the next permutation.
     *
     * @param permuter A Permuter instance that generates permutations of configuration parameters.
     * @return A Map of key-value pairs representing the next permutation of configuration parameters.
     * @throws NoSuchElementException If the next permutation is not available and loopBack is not set.
     */
    private Map<String, String> parameterRoutine( Permuter permuter ) {
        Optional<Map<String, String>> parameters = permuter.next();
        if ( parameters.isEmpty() && loopBack ) {
            permuter.loopBack();
            return permuter.next().orElseThrow();
        }
        return permuter.next().orElseThrow();
    }

    /**
     * Creates a new DataConfig instance using the current generator configuration.
     *
     * @return A new DataConfig instance.
     */
    public DataConfig createDataConfig() {
        return new DataConfig( parameterRoutine( dataConfigPermuter ) );
    }

    public QueryConfig createQueryConfig() {
        // Todo handle weights and complexity
        return new QueryConfig( parameterRoutine( queryConfigPermuter ), null, 4 );
    }

    public StoreConfig createStoreConfig() {
        return new StoreConfig( parameterRoutine( storeConfigPermuter ), null );
    }

    public PartitionConfig createPartitionConfig() {
        return new PartitionConfig( parameterRoutine( partitionConfigPermuter ) );
    }

    public StartConfig createStartConfig() {
        return new StartConfig( parameterRoutine( startConfigPermuter ) );
    }

    public SchemaConfig createSchemaConfig() {
        return this.schemaConfig;
    }

    synchronized public Profile createProfile( SeedsConfig seedsConfig ) {
        counter++;
        return Profile.builder()
                .schemaConfig(createSchemaConfig())
                .startConfig(createStartConfig())
                .queryConfig(createQueryConfig())
                .dataConfig(createDataConfig())
                .storeConfig(createStoreConfig())
                .partitionConfig(createPartitionConfig())
                .issuedSeeds(seedsConfig)
                .build();
    }

    public static ProfileGenerator getProfileGenerator() {
        // Todo add manual configuration -> UI
        return new PolyfierProfiler()
                .addPreset(ConfigType.DATA, "dataConfig0", "")
                .addPreset(ConfigType.DATA, "dataConfig1", "")
                .addPreset(ConfigType.DATA, "dataConfig2", "")
                .addPreset(ConfigType.DATA, "dataConfig3", "")
                .addPreset(ConfigType.QUERY, "queryConfig0", "")
                .addPreset(ConfigType.QUERY, "queryConfig1", "")
                .addPreset(ConfigType.QUERY, "queryConfig2", "")
                .addPreset(ConfigType.QUERY, "queryConfig3", "")
                .addPreset(ConfigType.STORE, "CASSANDRA", "false")
                .addPreset(ConfigType.STORE, "NEO4J", "false")
                .addPreset(ConfigType.STORE, "COTTONTAIL", "false")
                .addPreset(ConfigType.PART, "partConfig0", "")
                .addPreset(ConfigType.PART, "partConfig1", "")
                .addPreset(ConfigType.PART, "partConfig2", "")
                .addPreset(ConfigType.PART, "partConfig3", "")
                .addPreset(ConfigType.START, "startConfig0", "")
                .addPreset(ConfigType.START, "startConfig1", "")
                .addPreset(ConfigType.START, "startConfig2", "")
                .addPreset(ConfigType.START, "startConfig3", "")
                .addPerm(ConfigType.STORE, "HSQLDB")
                .addPerm(ConfigType.STORE, "POSTGRESQL")
                .addPerm(ConfigType.STORE, "MONGODB")
                .addPerm(ConfigType.STORE, "MONETDB")
                .build();
    }

    private static class PolyfierProfiler implements javafx.util.Builder<ProfileGenerator> {

        private final Map<ConfigType, Map<String, String>> presets = new HashMap<>();
        private final Map<ConfigType, List<String>> permeable = new HashMap<>();

        public PolyfierProfiler() {
            for (ConfigType type : ConfigType.values()) {
                presets.put(type, new HashMap<>());
                permeable.put(type, new LinkedList<>());
            }
        }

        public PolyfierProfiler addPreset(ConfigType type, String key, String val) {
            presets.get(type).put(key, val);
            return this;
        }

        public PolyfierProfiler addPerm(ConfigType type, String key) {
            permeable.get(type).add(key);
            return this;
        }

        @Override
        public ProfileGenerator build() {
            return ProfileGenerator.create(
                    permeable.get(ConfigType.DATA),
                    presets.get(ConfigType.DATA),
                    permeable.get(ConfigType.QUERY),
                    presets.get(ConfigType.QUERY),
                    permeable.get(ConfigType.STORE),
                    presets.get(ConfigType.STORE),
                    permeable.get(ConfigType.PART),
                    presets.get(ConfigType.PART),
                    permeable.get(ConfigType.START),
                    presets.get(ConfigType.START),
                    false
            );
        }
    }


}
