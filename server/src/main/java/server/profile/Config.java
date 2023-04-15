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

package server.profile;

import com.google.gson.GsonBuilder;
import connect.ConfigType;
import org.apache.commons.codec.digest.MurmurHash2;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;
import java.util.Arrays;
import java.util.StringJoiner;

/**
 * The {@link Config} class provides a base for various configurations in the test-database system.
 * Each configuration represents some input or output and is JSON Serializable.
 */
public abstract class Config implements Serializable {

    private transient ConfigType configType;

    /**
     * Default constructor that processes the configuration object.
     */
    protected Config() {
        processConfig( this );
    }

    /**
     * Processes the given configuration object and sets the appropriate {@link ConfigType}.
     *
     * @param config The configuration object to process.
     * @throws IllegalArgumentException if an invalid configuration type is provided.
     */
    public void processConfig( Config config ) {
        if (config instanceof SchemaConfig) {
            this.configType = ConfigType.SCHEMA;
        } else if (config instanceof DataConfig) {
            this.configType = ConfigType.DATA;
        } else if (config instanceof QueryConfig) {
            this.configType = ConfigType.QUERY;
        } else if (config instanceof StoreConfig) {
            this.configType = ConfigType.STORE;
        } else if (config instanceof StartConfig) {
            this.configType = ConfigType.START;
        } else if (config instanceof PartitionConfig) {
            this.configType = ConfigType.PART;
        } else if (config instanceof LogicalPlanConfig) {
            this.configType = ConfigType.LOGICAL_PLAN;
        } else if (config instanceof PhysicalPlanConfig) {
            this.configType = ConfigType.PHYSICAL_PLAN;
        } else if (config instanceof SeedsConfig) {
            this.configType = ConfigType.SEEDS;
        } else if (config instanceof ErrorConfig) {
            this.configType = ConfigType.ERROR;
        } else {
            throw new IllegalArgumentException("Invalid Config Enum given.");
        }
    }

    /**
     * Returns the {@link ConfigType} of the current configuration object.
     *
     * @return The configuration type.
     */
    public ConfigType getType() {
        return this.configType;
    };


    /**
     * Returns a {@link Pair} containing the hash and the JSON string representation
     * of the current configuration object.
     */
    public Pair<Long, String> hashAndString() {
        String jsonString = new GsonBuilder().create().toJson( this );
        return Pair.of( hash( jsonString ), jsonString );
    }

    /**
     * Generates a hash from the given list of hashes.
     *
     * @param hashes An array of hashes to be combined.
     * @return The combined hash.
     */
    public static long rehash( Long ... hashes ) {
        StringJoiner sj = new StringJoiner( "&" );
        Arrays.stream(hashes).map(Object::toString).toList().forEach(sj::add);
        return hash( sj.toString() );
    }

    /**
     * Generates a hash from the given data string.
     *
     * @param data The input data to be hashed.
     * @return The hash value of the input data.
     */
    private static long hash( String data ) {
        // everything hashes here
        return MurmurHash2.hash64( data );
    }

}
