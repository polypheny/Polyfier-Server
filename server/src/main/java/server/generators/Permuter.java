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

import java.util.Map;
import java.util.Optional;

/**
 * The Permuter interface provides methods to handle permutation operations
 * for a given set of configurations. Implementations of this interface
 * should be responsible for generating permutations and managing iteration
 * through the available permutations.
 */
public interface Permuter {

    /**
     * Returns the next Permutation of a configuration.
     */
    Optional<Map<String, String>> next();

    /**
     * Returns the next Permutation of a configuration such that the same will be returned on the next call.
     */
    Optional<Map<String, String>> peek();

    /**
     * Resets the Permuter such that it will return the permutations in the same order from the beginning on the next call.
     */
    void loopBack();

}
