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

package server.messages;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import io.javalin.websocket.WsContext;
import io.javalin.websocket.WsMessageContext;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Triple;

import java.io.Serializable;

@Getter
@Slf4j
public class ClientMessage implements Serializable {
    @Setter
    private transient WsContext wsContext;
    private String apiKey;
    private ClientCode clientCode;
    private String branch;
    private MessageCode messageCode;
    private String body;

    public ClientMessage() {}

    public ClientMessage( String apiKey, String branch, String clientCode, String messageCode, String body ) {
        this.apiKey = apiKey;
        this.clientCode = ClientCode.valueOf( clientCode );
        this.branch = branch;
        this.messageCode = MessageCode.valueOf( messageCode );
        this.body = body;

        if ( this.messageCode.getClientCode() != this.clientCode ) {
            throw new IllegalArgumentException( "MessageCode " + messageCode + " not supported for ClientCode: " + clientCode );
        }
    }

    @Getter
    public static class Key implements Serializable {
        private String key;
    }

    @Getter
    public static class KeyPair implements Serializable {
        private String key1;
        private String key2;
    }

    @Getter
    public static class KeyStatus implements Serializable {
        private String key;
        private String status;
    }


    @Getter
    public static class PDBResult implements Serializable {
        private String pdbKey;
        private Long seed;
        private Long resultSetHash;
        private Boolean success;
        private String error;
        private String logical;
        private String physical;
        private Long actual;
        private Long predicted;
    }

    private enum ClientCode {
        PCTRL,
        PDB,
        BROWSER
    }

    @Getter
    @AllArgsConstructor
    private enum MessageCode {
        // PCTRL
        PCTRL_SIGN_IN( ClientCode.PCTRL, Key.class ),
        PCTRL_SIGN_OUT( ClientCode.PCTRL, Key.class ),
        PCTRL_STATUS_UPD( ClientCode.PCTRL, KeyStatus.class ),
        PCTRL_REQ_JOB( ClientCode.PCTRL, KeyPair.class ),
        // PDB
        PDB_SIGN_IN( ClientCode.PDB, Key.class ),
        PDB_SIGN_OUT( ClientCode.PDB, Key.class ),
        PDB_REQ_JOB( ClientCode.PDB, Key.class ),
        PDB_STATUS_UPD( ClientCode.PDB, KeyStatus.class ),
        PDB_RESULT_DEP( ClientCode.PDB, PDBResult.class ),
        // BROWSER
        BROWSER_LOG( ClientCode.BROWSER, null ),
        BROWSER_SYS( ClientCode.BROWSER, null );

        private final ClientCode clientCode;
        private final Class<?> clazz;

    }

    private static Triple<ClientMessage, Object, Integer> read(String message ) throws JsonSyntaxException {
        ClientMessage clientMessage = null;
        try {
            log.debug(message);
            Gson gson = new Gson();
            clientMessage = gson.fromJson( message, ClientMessage.class );

            if ( clientMessage.getClientCode() == ClientCode.BROWSER ) {
                return Triple.of( clientMessage, null, 0 );
            }

            return Triple.of( clientMessage, gson.fromJson( clientMessage.getBody(), clientMessage.getMessageCode().getClazz() ), clientMessage.getBody().length() );

        } catch (Exception e) {
            if ( clientMessage != null ) {
                log.debug(new Gson().toJson(clientMessage));
            }
            throw new RuntimeException(e);
        }

    }

    private static boolean verifyApiKey( String apiKey ) {
        log.debug("Verifying: " + apiKey);
        // Todo;
        return true;
    }

    public static void processMessage( WsMessageContext session ) {
        // Read Message according to API Specifications.
        Triple<ClientMessage, Object, Integer> triple = ClientMessage.read( session.message() );
        ClientMessage clientMessage = triple.getLeft();
        clientMessage.setWsContext( session );

        switch ( clientMessage.getClientCode() ) {
            case PCTRL -> {
                if ( log.isDebugEnabled() ) {
                    log.debug("PCTRL: " + clientMessage.getMessageCode() );
                }
                if ( ! verifyApiKey( clientMessage.getApiKey() ) ) {
                    throw new IllegalArgumentException("Invalid API Key");
                }
                switch ( clientMessage.getMessageCode() ) {
                    case PCTRL_SIGN_IN -> {
                        ServerMessage.handlePCTRLSignIn( clientMessage, ((Key) triple.getMiddle()).getKey() );
                    }
                    case PCTRL_SIGN_OUT -> {
                        ServerMessage.handlePCTRLSignOut( clientMessage, ((Key) triple.getMiddle()).getKey() );
                    }
                    case PCTRL_STATUS_UPD -> {
                        KeyStatus keyStatus = ((KeyStatus) triple.getMiddle());
                        ServerMessage.handlePCTRLStatus( clientMessage, keyStatus.getKey(), keyStatus.getStatus() );
                    }
                    case PCTRL_REQ_JOB -> {
                        KeyPair keyPair = ((KeyPair) triple.getMiddle());
                        ServerMessage.handlePCTRLJob( clientMessage, keyPair.getKey1(), keyPair.getKey2() );
                    }
                }
            }
            case PDB -> {
                if ( log.isDebugEnabled() ) {
                    log.debug("PDB: " + clientMessage.getMessageCode() );
                }
                if ( ! verifyApiKey( clientMessage.getApiKey() ) ) {
                    throw new IllegalArgumentException("Invalid API Key");
                }
                switch ( clientMessage.getMessageCode() ) {
                    case PDB_SIGN_IN -> {
                        ServerMessage.handlePDBSignIn( clientMessage, ((Key) triple.getMiddle()).getKey() );
                    }
                    case PDB_SIGN_OUT -> {
                        ServerMessage.handlePDBSignOut( clientMessage, ((Key) triple.getMiddle()).getKey() );
                    }
                    case PDB_REQ_JOB -> {
                        ServerMessage.handlePDBJob( clientMessage, ((Key) triple.getMiddle()).getKey() );
                    }
                    case PDB_STATUS_UPD -> {
                        KeyStatus keyStatus = ((KeyStatus) triple.getMiddle());
                        ServerMessage.handlePDBStatus( clientMessage, keyStatus.getKey(), keyStatus.getStatus() );
                    }
                    case PDB_RESULT_DEP -> {
                        ServerMessage.handlePDBResult( clientMessage, ((PDBResult) triple.getMiddle()), triple.getRight() );
                    }
                }
            }
            case BROWSER -> {
                if ( log.isDebugEnabled() ) {
                    log.debug("BROWSER: " + clientMessage.getMessageCode() );
                }
                switch ( clientMessage.getMessageCode() ) {
                    case BROWSER_LOG -> {
                        ServerMessage.unsubscribeSys( session );
                        ServerMessage.subscribeLog( session );
                    }
                    case BROWSER_SYS -> {
                        ServerMessage.unsubscribeLog( session );
                        ServerMessage.subscribeSys( session );
                    }
                }
            }
        }
    }

    public static void handleConnectionLoss( WsContext wsContext ) {
        log.debug( "Handling Connection Loss: " + wsContext.getSessionId() );
        // Todo
    }


}
