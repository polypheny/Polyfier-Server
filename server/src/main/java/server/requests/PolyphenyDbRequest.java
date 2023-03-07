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
public class PolyphenyDbRequest implements Serializable {

    private final String apiKey;
    private final String cookie;

    public static class Task extends PolyphenyDbRequest {

        public Task( String apiKey, String cookie ) {
            super(apiKey, cookie);
        }

    }

    @Getter
    public static class Result extends PolyphenyDbRequest {

        String orderId;
        Long seed;
        Long resultSetHash;
        Boolean success;
        String error;
        String logical;
        String physical;
        Long actual;
        Long predicted;

        public Result(String apiKey, String cookie) {
            super(apiKey, cookie);
        }
    }


}
