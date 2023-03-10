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

package server.generators;

import server.config.SeedsConfig;
import lombok.Builder;
import server.PolyphenyDbProfile;
import server.config.*;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;


@Builder
public class PolyphenyDbProfileGenerator implements Serializable {

    private transient Permuter dataConfigPermuter;
    private List<String> dataConfigPermeable;
    private HashMap<String, String> dataConfigPreset;

    private transient Permuter queryConfigPermuter;
    private List<String> queryConfigPermeable;
    private HashMap<String, String> queryConfigPreset;


    private transient Permuter storeConfigPermuter;
    private List<String> storeConfigPermeable;
    private HashMap<String, String> storeConfigPreset;


    private transient Permuter partitionConfigPermuter;
    private List<String> partitionConfigPermeable;
    private HashMap<String, String> partitionConfigPreset;

    private transient Permuter startConfigPermuter;
    private List<String> startConfigPermeable;
    private HashMap<String, String> startConfigPreset;

    private SchemaConfig schemaConfig;

    private boolean loopBack; // Else Throw

    private long counter;

    public static PolyphenyDbProfileGenerator create(
            List<String> dataConfigPermeable,
            HashMap<String, String> dataConfigPreset,
            List<String> queryConfigPermeable,
            HashMap<String, String> queryConfigPreset,
            List<String> storeConfigPermeable,
            HashMap<String, String> storeConfigPreset,
            List<String> partitionConfigPermeable,
            HashMap<String, String> partitionConfigPreset,
            List<String> startConfigPermeable,
            HashMap<String, String> startConfigPreset,
            boolean loopBack
    ) {
        // Todo Expand
        return PolyphenyDbProfileGenerator.builder()
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

    private Map<String, String> parameterRoutine( Permuter permuter ) {
        Optional<Map<String, String>> parameters = permuter.next();
        if ( parameters.isEmpty() && loopBack ) {
            permuter.loopBack();
            return permuter.next().orElseThrow();
        }
        return permuter.next().orElseThrow();
    }

    public DataConfig createDataConfig() {
        return new DataConfig( (HashMap<String, String>) parameterRoutine( dataConfigPermuter ));
    }

    public QueryConfig createQueryConfig() {
        // Todo handle weights and complexity
        return new QueryConfig( (HashMap<String, String>) parameterRoutine( queryConfigPermuter ), null, 0 );
    }

    public StoreConfig createStoreConfig() {
        return new StoreConfig( (HashMap<String, String>) parameterRoutine( storeConfigPermuter), null );
    }

    public PartitionConfig createPartitionConfig() {
        return new PartitionConfig( (HashMap<String, String>) parameterRoutine( partitionConfigPermuter) );
    }

    public StartConfig createStartConfig() {
        return new StartConfig( (HashMap<String, String>) parameterRoutine( startConfigPermuter) );
    }

    public SchemaConfig createSchemaConfig() {
        return this.schemaConfig;
    }

    public PolyphenyDbProfile createProfile( String profileKey, String apiKey, SeedsConfig seedsConfig) {
        counter++;
        return PolyphenyDbProfile.builder()
                .profileKey( profileKey )
                .apiKey( apiKey )
                .schemaConfig( createSchemaConfig() )
                .startConfig( createStartConfig() )
                .queryConfig( createQueryConfig() )
                .dataConfig( createDataConfig() )
                .storeConfig( createStoreConfig() )
                .partitionConfig( createPartitionConfig() )
                .createdAt( new Timestamp( System.currentTimeMillis() ) )
                .completedAt( null )
                .issuedSeeds(seedsConfig)
                .build();
    }


}
