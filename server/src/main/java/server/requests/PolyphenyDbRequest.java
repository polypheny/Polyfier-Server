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

package server.requests;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serializable;


@AllArgsConstructor
@Getter
public class PolyphenyDbRequest implements Serializable {

    private final String apiKey;
    private final String orderKey;

    /**
     * Mirrors the request-body send by Polypheny-DB in the class PolyfierProcess at job request.
     */
    public static class Job extends PolyphenyDbRequest {
        public Job( String apiKey, String orderKey ) {
            super( apiKey, orderKey );
        }
    }

    /**
     * Mirrors the class PolyfierResult in the polyfier module of Polypheny-DB.
     */
    @Getter
    public static class Result extends PolyphenyDbRequest {
        public Long seed;
        public Long resultSetHash;
        public Boolean success;
        public String error;
        public String logical;
        public String physical;
        public Long actual;
        public Long predicted;

        public Result( String apiKey, String orderKey ) {
            super( apiKey, orderKey );
        }
    }


}
