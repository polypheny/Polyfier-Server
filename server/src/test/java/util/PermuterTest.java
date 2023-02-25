package util;

import org.junit.Test;
import org.junit.Assert;

import java.util.*;

public class PermuterTest {

    @Test
    public void permuterReturnsCorrectPermutations() {

        HashSet<String> supportedStores = new HashSet<>( Set.of(
                "MongoDB",
                "PostgreSQL",
                "HSQLDB",
                "Cassandra"
        ) );

        HashSet<String> requestedStores = new HashSet<>( Set.of("HSQLDB", "PostgreSQL", "Cassandra") );

        HashMap<String, String> preConfig = new HashMap<>();
        preConfig.put("HSQLDB", "false");

        BooleanPermuter permuter = new BooleanPermuter( supportedStores, requestedStores, true, preConfig, "bit-value" );

        for (int i = 0; i < 8; i++ ) {
            Map<String, String> permutation = permuter.get();
            Assert.assertNotNull("Permutation cant be null.", permutation);
            System.out.println( i + ": " + permutation );
        }

    }


}
