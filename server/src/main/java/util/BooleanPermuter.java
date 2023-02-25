package util;

import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Permutation generator for boolean configurations. For a given set of possible configurations and a set of requested configurations
 * the Permuter implements a Supplier that returns a Map of configurations to Booleans, either on or off. The Map will contain
 * the set of all possible configurations, marked as either on or off, and will permute the requested configurations.
 * Alternatively a pre-configuration of some values can be provided.
 */
public class BooleanPermuter implements Supplier<Map<String, String>> {
    private static final String BIT_VALUE = "bit-value";
    private static final String UNSET_BIT_COUNT = "unset-bit-count";
    private static final String SET_BIT_COUNT = "set-bit-count";
    private final Iterator<String> permutationStreamIterator;
    private List<String> configurations;
    private final String sortMode;
    private final Map<String, String> preConfiguration;

    public BooleanPermuter( HashSet<String> possibleConfigurations, HashSet<String> requestedConfigurations, @Nullable Boolean defaultValue, @Nullable Map<String, String> preConfiguration, @Nullable String sortMode ) {
        Tuple<Integer, Integer> partition = prepareStorePermutation( possibleConfigurations, requestedConfigurations );
        Stream<String> permutationsStream = getPermutationStream( partition.left, partition.right, Boolean.TRUE.equals(defaultValue) );
        this.permutationStreamIterator = permutationsStream.iterator();
        this.sortMode = (sortMode == null) ? SET_BIT_COUNT: sortMode;
        this.preConfiguration = preConfiguration;
    }

    private Tuple<Integer, Integer> prepareStorePermutation( HashSet<String> possibleConfigurations, HashSet<String> requestedConfigurations ) {
        if ( ! possibleConfigurations.containsAll( requestedConfigurations ) ) {
            throw new IllegalArgumentException( "One or more requested configurations are not possible." );
        }
        possibleConfigurations.removeAll( requestedConfigurations );
        Tuple<Integer, Integer> partition = new Tuple<>( possibleConfigurations.size(), requestedConfigurations.size() );
        ArrayList<String> configurations = new ArrayList<>( requestedConfigurations );
        configurations.addAll( possibleConfigurations );
        this.configurations = configurations;
        return partition;
    }

    private Stream<String> getPermutationStream( final int ignoredConfigurations, final int requestedConfigurations, boolean defaultValue ) {
        return IntStream.range( 0, ( 1 << requestedConfigurations)).boxed().sorted( (i, j) -> {
            switch ( sortMode ) {
                case SET_BIT_COUNT -> {
                    int n = countBits( i );
                    int m = countBits( j );
                    return Integer.compare( n, m );
                }
                case UNSET_BIT_COUNT -> {
                    int n = countBits(i) - String.valueOf(i).length();
                    int m = countBits(j) - String.valueOf(j).length();
                    return Integer.compare(n, m);
                }
                case BIT_VALUE -> {
                    return Integer.compare( i, j );
                }
                default -> throw new IllegalArgumentException( "Unknown sorting mode:" + sortMode );
            }
        }).map( Integer::toBinaryString ).map(
                s -> {
                    s = "0".repeat( Math.abs(s.length() - requestedConfigurations) ) + s + String.valueOf( ( defaultValue ) ? 1:0 ).repeat( ignoredConfigurations );
                    System.out.println(s);
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

    @Override
    public Map<String, String> get() {
        if ( ! this.permutationStreamIterator.hasNext() ) {
            return null;
        }
        final List<Integer> permutation = Arrays.stream( this.permutationStreamIterator.next().split("") ).map(Integer::valueOf).toList();
        Map<String, String> configurations = IntStream.range( 0, this.configurations.size() ).boxed().collect(
                Collectors.toMap( this.configurations::get, i -> (permutation.get( i ) == 1) ? "true":"false"  )
        );
        if ( this.preConfiguration != null ) {
            this.preConfiguration.keySet().forEach( key -> {
                configurations.put( key, preConfiguration.get( key) );
            });
        }
        return configurations;
    }

}
