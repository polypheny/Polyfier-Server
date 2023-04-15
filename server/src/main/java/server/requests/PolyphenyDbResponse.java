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

import lombok.Getter;
import server.profile.*;

import java.io.Serializable;

public class PolyphenyDbResponse implements Serializable {

    @Getter
    public static class Job extends PolyphenyDbResponse {
        private final SchemaConfig schemaConfig;
        private final DataConfig dataConfig;
        private final QueryConfig queryConfig;
        private final StoreConfig storeConfig;
        private final PartitionConfig partitionConfig;
        private final SeedsConfig seedsConfig;

        public Job(
                SchemaConfig schemaConfig,
                DataConfig dataConfig,
                QueryConfig queryConfig,
                StoreConfig storeConfig,
                PartitionConfig partitionConfig,
                SeedsConfig seedsConfig
                ) {
            super();
            this.schemaConfig = schemaConfig;
            this.dataConfig = dataConfig;
            this.queryConfig = queryConfig;
            this.storeConfig = storeConfig;
            this.partitionConfig = partitionConfig;
            this.seedsConfig = seedsConfig;
        }

    }

}
