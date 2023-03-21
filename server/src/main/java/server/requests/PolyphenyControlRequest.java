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

@Getter
@AllArgsConstructor
public class PolyphenyControlRequest implements Serializable {

    private final String apiKey;

    /**
     * Request signaling the PolyphenyControl client is in either of three modes.
     * IDLE - If the client is ready for a job.
     * START - If the client is building a PolyphenyDB instance.
     * BUSY - If the PolyphenyDB instance that is managed by the client is running.
     */
    @Getter
    public static class KeepAlive extends PolyphenyControlRequest {
        private final String status;

        public KeepAlive( String apiKey , String status ) {
            super(apiKey);
            this.status = status;
        }
    }

    @Getter
    public static class SignIn extends PolyphenyControlRequest {
        public SignIn( String apiKey  ) { super(apiKey); }
    }

    public static class SignOut extends PolyphenyControlRequest {
        public SignOut(String apiKey ) {
            super(apiKey);
        }
    }

    public static class StartConfiguration extends PolyphenyControlRequest {
        public StartConfiguration( String apiKey ) {
            super(apiKey);
        }
    }

}
