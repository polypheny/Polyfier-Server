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
import server.config.StartConfig;

@AllArgsConstructor
public class PolyphenyControlResponse {

    public static class StartConfiguration extends PolyphenyControlResponse {
        String clientId;
        StartConfig startConfig;

        public StartConfiguration( StartConfig startConfig, String clientId ) {
            this.startConfig = startConfig;
            this.clientId = clientId;
        }

    }

}
