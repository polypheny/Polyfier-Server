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

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.Nullable;
import util.Tuple;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Permutation generator for boolean configurations. For a given set of possible configurations and a set of requested configurations
 * the Permuter implements a Supplier that returns a Map of configurations to Booleans, either on or off. The Map will contain
 * the set of all possible configurations, marked as either on or off, and will permute the requested configurations.
 * Alternatively a pre-configuration of some values can be provided.
 */
@Slf4j
public class BooleanPermuter implements Permuter {
    private List<String> configurations;
    private final Map<String, String> preConfiguration;
    private final String[] permutations;

    private int pos;

    public static BooleanPermuter from( @Nullable List<String> permeable, @Nullable Map<String, String> nonPermeable ) {
        if ( permeable == null || permeable.size() == 0 ) {
            assert nonPermeable != null;
            return new BooleanPermuter( nonPermeable );
        }

        HashSet<String> nonPermeableConfigurations;
        if ( nonPermeable == null ) {
            nonPermeableConfigurations = new HashSet<>();
            nonPermeable = new HashMap<>();
        } else {
            nonPermeableConfigurations = new HashSet<>(nonPermeable.keySet());
        }

        HashSet<String> permeableConfigurations = new HashSet<>( permeable );

        return new BooleanPermuter(
                nonPermeableConfigurations,
                permeableConfigurations,
                nonPermeable
        );
    }

    private BooleanPermuter( @NonNull Map<String, String> preConfigured ) {
        this.permutations = null;
        this.preConfiguration = preConfigured;
    }

    private BooleanPermuter( @NonNull HashSet<String> nonPermeableConfigurations, @NonNull HashSet<String> permeableConfigurations, @NonNull Map<String, String> preConfiguration ) {
        Tuple<Integer, Integer> partition = preparePermutation( nonPermeableConfigurations, permeableConfigurations );
        this.permutations = getPermutationStream( partition.left, partition.right ).toArray( String[]::new );
        this.preConfiguration = preConfiguration;
    }

    private Tuple<Integer, Integer> preparePermutation( @NonNull HashSet<String> nonPermeableConfigurations, @Nullable HashSet<String> permeableConfigurations ) {
        assert permeableConfigurations != null;
        assert permeableConfigurations.stream().noneMatch( nonPermeableConfigurations::contains );

        Tuple<Integer, Integer> partition = new Tuple<>( nonPermeableConfigurations.size(), permeableConfigurations.size() );
        ArrayList<String> configurations = new ArrayList<>( permeableConfigurations );
        configurations.addAll( nonPermeableConfigurations );

        this.configurations = configurations;

        return partition;
    }

    private Stream<String> getPermutationStream( final int nonPermeableConfigurationsNo, final int permeableConfigurationsNo ) {
        if ( log.isDebugEnabled() ) {
            log.debug("Creating Permutations for " + permeableConfigurationsNo + " permeable configurations...");
            log.debug("Integer values for bits range from 0 to " + ( 1 << permeableConfigurationsNo ) );
        }
        return IntStream.range( 0, ( 1 << permeableConfigurationsNo ) ).boxed().sorted( ( i, j ) -> {
                int n = countBits( i );
                int m = countBits( j );
                return Integer.compare( n, m );
        }).map( Integer::toBinaryString ).map(
                s -> {
                    s = "0".repeat( Math.abs(s.length() - permeableConfigurationsNo ) ) + s + "0".repeat( nonPermeableConfigurationsNo );
                    if ( log.isDebugEnabled() ) {
                        log.debug("\t Permuted Bits: b'" + s + "'");
                    }
                    return s;
                }
        );
    }

    private int countBits( int i ) {
        int n = i;
        int m = 0;
        while ( n > 0 ) {
            m += n & 1;
            n >>= 1;
        }
        return m;
    }

    public void loopBack() {
        this.pos = 0;
    }

    public Optional<Map<String, String>> next() {
        return next( false );
    }

    public Optional<Map<String, String>> peek() {
        return next( true );
    }

    private Optional<Map<String, String>> next( boolean peek ) {
        if ( this.permutations == null ) {
            return Optional.of( this.preConfiguration );
        }
        if ( this.pos >= this.permutations.length ) {
            return Optional.empty();
        }
        final List<Integer> permutation = Arrays.stream( this.permutations[this.pos].split("") ).map(Integer::valueOf).toList();
        Map<String, String> configurations = IntStream.range( 0, this.configurations.size() ).boxed().collect(
                Collectors.toMap( this.configurations::get, i -> (permutation.get( i ) == 1) ? "true":"false"  )
        );
        if ( this.preConfiguration != null ) {
            this.preConfiguration.keySet().forEach( key -> {
                configurations.put( key, preConfiguration.get( key) );
            });
        }
        if ( ! peek ) {
            this.pos++;
        }
        return Optional.of( configurations );
    }


}
