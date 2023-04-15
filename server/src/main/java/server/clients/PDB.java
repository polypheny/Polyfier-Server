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

package server.clients;


import io.javalin.websocket.WsContext;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Map;

@Getter
@Setter
@AllArgsConstructor
public class PDB implements Serializable {
    private transient WsContext wsContext;

    private int resultsCount; // For Browser
    private double dataCount;  // For Browser
    private Map<String, String> dataStores; // For Browser

    private String pdbKey;
    private String pctrlKey;
    private String sessionId;
    private String branch;
    private String status;
    private boolean active;

    private long registeredAt;
    private long updateTime;

    public void incrementResultCount() {
        this.resultsCount++;
    }

    public void addDataCount( double data ) {
        dataCount += data;
    }

}
