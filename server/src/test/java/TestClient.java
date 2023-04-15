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

import com.google.gson.Gson;
import jakarta.websocket.*;
import javafx.util.Pair;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import server.profile.Profile;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;


@Slf4j
public class TestClient {
    static Thread mockServer;

    @BeforeAll
    public static void setUp() {
        mockServer = new Thread( () -> PolyfierServer.runServer( "0.0.0.0", 44567 ) );
        mockServer.setDaemon( true );
        mockServer.start();
        try {
            Thread.sleep( 20000 );
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testServer() {
        try {
            testRoutine();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Getter(AccessLevel.PRIVATE)
    public static class PolyfierClientPctrl {
        private final Gson gson = new Gson();
        public final PctrlWebSocketEndPoint pctrlWebSocketEndPoint;
        private final PctrlUpdateRoutineHandler pctrlUpdateRoutineHandler;
        private final String apiKey;
        private final String polyphenyControlBranch;
        private final UUID polyfierClientId;
        private final Object jobLock = new Object();
        @Setter
        private Profile nextJob;

        PolyfierClientPctrl( URI polyfierUri, String apiKey, String polyphenyControlBranch ) {
            this.apiKey = apiKey;
            this.polyfierClientId = UUID.randomUUID();
            this.polyphenyControlBranch = polyphenyControlBranch;
            this.pctrlWebSocketEndPoint = new PctrlWebSocketEndPoint( polyfierUri, (message, session) -> {

                // Handle messages from server.

                log.debug("Handle Message: " + message);

                if (! Objects.equals(message, "OK") ) {
                    synchronized ( getJobLock() ) {
                        this.nextJob = gson.fromJson(message, Profile.class);
                        this.jobLock.notify();
                    }
                }

            } );
            this.pctrlUpdateRoutineHandler = new PctrlUpdateRoutineHandler(
                    getPctrlWebSocketEndPoint(),
                    apiKey,
                    getPolyfierClientId().toString(),
                    "IDLE",
                    true
            );
            Thread pctrlUpdateRoutineThread = new Thread( getPctrlUpdateRoutineHandler() );
            pctrlUpdateRoutineThread.setDaemon( true );
            pctrlUpdateRoutineThread.start();

            this.signIn();
        }

        private void signIn() {
            getPctrlWebSocketEndPoint().sendMessage( getGson().toJson(
                    new PctrlSignInMessage( getApiKey(), getPolyphenyControlBranch(), getPolyfierClientId().toString() )
            ) );
        }

        private void signOut() {
            getPctrlWebSocketEndPoint().sendMessage( getGson().toJson(
                    new PctrlSignOutMessage( getApiKey(), getPolyfierClientId().toString() )
            ) );
        }

        public Pair<UUID, Profile> requestJob() throws InterruptedException {
            if ( getNextJob() != null ) {
                setNextJob( null );
            }

            final UUID uuid = UUID.randomUUID(); // PolyphenyDB Identifier

            getPctrlWebSocketEndPoint().sendMessage( getGson().toJson(
                    new PctrlReqJobMessage( getApiKey(), getPolyfierClientId().toString(), uuid.toString() )
            ) );

            synchronized ( getJobLock() ) {
                getJobLock().wait( 60000 ); // One Minute timeout. Waiting for Polyfier response to Job Request.
            }

            assert getNextJob() != null;

            return new Pair<>( uuid, getNextJob() );
        }

        public void changeStatus( String status ) {
            this.getPctrlUpdateRoutineHandler().setStatus( status );
        }

        public void shutdown() {
            this.getPctrlUpdateRoutineHandler().setRunning( false );
            try {
                Thread.sleep( 200 );
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            this.signOut();
        }

        public interface MessageHandler {
            void handleMessage( String message, Session session );
        }

        @ClientEndpoint
        public static class PctrlWebSocketEndPoint {
            private final MessageHandler messageHandler;
            private Session session;

            public PctrlWebSocketEndPoint( URI server, MessageHandler messageHandler ) {
                WebSocketContainer webSocketContainer = ContainerProvider.getWebSocketContainer();
                try {
                    this.session = webSocketContainer.connectToServer(this, server);
                    this.messageHandler = messageHandler;
                } catch (DeploymentException | IOException e ) {
                    throw new RuntimeException(e);
                }
            }


            public void sendMessage( String message ) {
                this.session.getAsyncRemote().sendText( message );
            }

            @OnOpen
            public void onOpen(Session session) {
                log.debug("Connected to WebSocket server: " + session.getId());
                this.session = session;
            }

            @OnMessage
            public void onMessage( String message, Session session ) {
                log.debug("Received message: " + message);
                if ( this.messageHandler != null ) {
                    this.messageHandler.handleMessage( message, session );
                } else {
                    log.warn("Message was not processed, no message-handler found:" + message );
                }
            }

            @OnClose
            public void onClose(Session session) {
                log.debug("WebSocket connection closed: " + session.getId());
                this.session = null;
            }

            @OnError
            public void onError(Throwable error) {
                log.error("WebSocket error: " + error.getMessage());
            }

        }

        @Getter
        @AllArgsConstructor
        public static class PctrlUpdateRoutineHandler implements Runnable {
            public final PctrlWebSocketEndPoint pctrlWebSocketEndpoint;
            private final String apiKey;
            private final String key;
            @Setter
            private String status;
            @Setter
            private boolean running;

            @Override
            public void run() {
                while ( isRunning() ) {
                    try {
                        Thread.sleep( 5000 );
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    getPctrlWebSocketEndpoint().sendMessage( new Gson().toJson( new PctrlStatusUpdMessage(
                            getApiKey(),
                            getKey(),
                            getStatus()
                    ) ) );
                }
            }
        }


        @Getter
        private static class PctrlSignInMessage implements Serializable {
            private final String apiKey;
            private final String clientCode;
            private final String branch;
            private final String messageCode;
            private final String body;

            public PctrlSignInMessage(String apiKey, String branch, String pctrlKey) {
                this.apiKey = apiKey;
                this.clientCode = "PCTRL";
                this.branch = branch;
                this.messageCode = "PCTRL_SIGN_IN";
                this.body = new Gson().toJson( new Body(pctrlKey) );
            }

            @Getter
            public static class Body implements Serializable {
                private final String key;

                public Body(String pctrlKey) {
                    this.key = pctrlKey;
                }
            }
        }

        @Getter
        private static class PctrlSignOutMessage implements Serializable {
            private final String apiKey;
            private final String clientCode;
            private final String messageCode;
            private final String body;

            public PctrlSignOutMessage(String apiKey, String pctrlKey) {
                this.apiKey = apiKey;
                this.clientCode = "PCTRL";
                this.messageCode = "PCTRL_SIGN_OUT";
                this.body = new Gson().toJson(  new Body( pctrlKey ) );
            }

            @Getter
            public static class Body implements Serializable {
                private final String key;

                public Body(String pctrlKey) {
                    this.key = pctrlKey;
                }
            }
        }

        @Getter
        private static class PctrlStatusUpdMessage implements Serializable {
            private final String apiKey;
            private final String clientCode;
            private final String messageCode;
            private final String body;

            public PctrlStatusUpdMessage(String apiKey, String pctrlKey, String status) {
                this.apiKey = apiKey;
                this.clientCode = "PCTRL";
                this.messageCode = "PCTRL_STATUS_UPD";
                this.body = new Gson().toJson(  new Body(pctrlKey, status) );
            }

            @Getter
            public static class Body implements Serializable {
                private final String key;
                private final String status;

                public Body(String pctrlKey, String status) {
                    this.key = pctrlKey;
                    this.status = status;
                }
            }
        }

        @Getter
        private static class PctrlReqJobMessage implements Serializable {
            private final String apiKey;
            private final String clientCode;
            private final String messageCode;
            private final String body;

            public PctrlReqJobMessage(String apiKey, String pctrlKey, String pdbKey) {
                this.apiKey = apiKey;
                this.clientCode = "PCTRL";
                this.messageCode = "PCTRL_REQ_JOB";
                this.body = new Gson().toJson(  new Body(pctrlKey, pdbKey) );
            }

            @Getter
            public static class Body implements Serializable {
                private final String key1;
                private final String key2;

                public Body(String pctrlKey, String pdbKey) {
                    this.key1 = pctrlKey;
                    this.key2 = pdbKey;
                }
            }
        }
    }

    @Getter(AccessLevel.PRIVATE)
    public static class PolyfierClientPdb {
        private final Gson gson = new Gson();
        public final PdbWebSocketEndPoint pdbWebSocketEndPoint;
        private final PdbUpdateRoutineHandler pdbUpdateRoutineHandler;
        private final String apiKey;
        private final UUID polyfierClientId;
        private final Object jobLock = new Object();

        @Setter
        private Profile nextJob;

        public PolyfierClientPdb( URI polyfierUri, String apiKey, UUID polyfierClientId ) {
            this.apiKey = apiKey;
            this.polyfierClientId = polyfierClientId;
            this.pdbWebSocketEndPoint = new PdbWebSocketEndPoint( polyfierUri, (message, session) -> {

                // Handle messages from server.

                log.debug("Handle Message: " + message);

                if (! Objects.equals(message, "OK") ) {
                    synchronized ( getJobLock() ) {
                        this.nextJob = getGson().fromJson(message, Profile.class);
                        getJobLock().notify();
                    }
                }

            } );
            this.pdbUpdateRoutineHandler = new PdbUpdateRoutineHandler(
                    getPdbWebSocketEndPoint(),
                    apiKey,
                    getPolyfierClientId().toString(),
                    "IDLE",
                    true
            );
            Thread pctrlUpdateRoutineThread = new Thread( getPdbUpdateRoutineHandler() );
            pctrlUpdateRoutineThread.setDaemon( true );
            pctrlUpdateRoutineThread.start();

            this.signIn();
        }

        private void signIn() {
            getPdbWebSocketEndPoint().sendMessage( getGson().toJson(
                    new PdbSignInMessage( getApiKey(), getPolyfierClientId().toString() )
            ) );
        }

        private void signOut() {
            getPdbWebSocketEndPoint().sendMessage( getGson().toJson(
                    new PdbSignOutMessage( getApiKey(), getPolyfierClientId().toString() )
            ) );
        }

        public Profile requestJob() throws InterruptedException {
            if ( getNextJob() != null ) {
                setNextJob( null );
            }

            getPdbWebSocketEndPoint().sendMessage( getGson().toJson(
                    new PdbReqJobMessage( getApiKey(), getPolyfierClientId().toString() )
            ) );

            synchronized ( getJobLock() ) {
                getJobLock().wait( 60000 ); // One Minute timeout. Waiting for Polyfier response to Job Request.
            }

            assert getNextJob() != null;

            return getNextJob();
        }

        public void changeStatus( String status ) {
            this.getPdbUpdateRoutineHandler().setStatus( status );
        }

        public void shutdown() {
            this.getPdbUpdateRoutineHandler().setRunning( false );
            try {
                Thread.sleep( 200 );
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            this.signOut();
        }

        public void depositResult(
                @NonNull Long seed,
                @NonNull Boolean success,
                @Nullable Long resultSetHash,
                @Nullable String error,
                @Nullable String logical,
                @Nullable String physical,
                @Nullable Long actual
        ) {
            getPdbWebSocketEndPoint().sendMessage( getGson().toJson( new PdbResultDepMessage(
                    getApiKey(),
                    getPolyfierClientId().toString(),
                    seed,
                    success,
                    resultSetHash,
                    error,
                    logical,
                    physical,
                    actual
            ) ) );
        }

        public interface MessageHandler {
            void handleMessage( String message, Session session );
        }

        @ClientEndpoint
        public static class PdbWebSocketEndPoint {
            private final MessageHandler messageHandler;
            private final Session session;

            public PdbWebSocketEndPoint( URI server, MessageHandler messageHandler ) {
                WebSocketContainer webSocketContainer = ContainerProvider.getWebSocketContainer();
                try {
                    this.session = webSocketContainer.connectToServer(this, server);
                    this.messageHandler = messageHandler;
                } catch (DeploymentException | IOException e ) {
                    throw new RuntimeException(e);
                }
            }

            public void sendMessage( String message ) {
                this.session.getAsyncRemote().sendText( message );
            }

            @OnOpen
            public void onOpen(Session session) {
                log.debug("Connected to WebSocket server: " + session.getId());
            }

            @OnMessage
            public void onMessage(String message, Session session) {
                log.debug("Received message: " + message);
                if (this.messageHandler != null) {
                    this.messageHandler.handleMessage(message, session);
                } else {
                    log.warn("Message was not processed, no message-handler found:" + message);
                }
            }

            @OnClose
            public void onClose(Session session) {
                log.debug("WebSocket connection closed: " + session.getId());
            }

            @OnError
            public void onError(Throwable error) {
                log.error("WebSocket error: " + error.getMessage());
            }
        }


        @AllArgsConstructor
        private static class PdbUpdateRoutineHandler implements Runnable {
            public final PdbWebSocketEndPoint pdbWebSocketEndPoint;
            private final String apiKey;
            private final String key;
            @Setter
            private String status;
            @Setter
            private boolean running;

            @Override
            public void run() {
                while ( running ) {
                    try {
                        Thread.sleep( 5000 );
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    pdbWebSocketEndPoint.sendMessage( new Gson().toJson( new PdbStatusUpdMessage( apiKey, key, status ) ) );

                }
            }
        }


        @Getter
        private static class PdbSignInMessage implements Serializable {
            private final String apiKey;
            private final String clientCode;
            private final String messageCode;
            private final String body;

            public PdbSignInMessage(String apiKey, String pdbKey) {
                this.apiKey = apiKey;
                this.clientCode = "PDB";
                this.messageCode = "PDB_SIGN_IN";
                this.body = new Gson().toJson( new Body(pdbKey) );
            }

            @Getter
            public static class Body implements Serializable {
                private final String key;

                public Body(String pdbKey) {
                    this.key = pdbKey;
                }
            }
        }
        @Getter
        private static class PdbSignOutMessage implements Serializable {
            private final String apiKey;
            private final String clientCode;
            private final String messageCode;
            private final String body;

            public PdbSignOutMessage(String apiKey, String pdbKey) {
                this.apiKey = apiKey;
                this.clientCode = "PDB";
                this.messageCode = "PDB_SIGN_OUT";
                this.body = new Gson().toJson( new Body(pdbKey) );
            }

            @Getter
            public static class Body implements Serializable {
                private final String key;

                public Body(String pdbKey) {
                    this.key = pdbKey;
                }
            }
        }


        @Getter
        private static class PdbStatusUpdMessage implements Serializable {
            private final String apiKey;
            private final String clientCode;
            private final String messageCode;
            private final String body;

            public PdbStatusUpdMessage(String apiKey, String pdbKey, String status) {
                this.apiKey = apiKey;
                this.clientCode = "PDB";
                this.messageCode = "PDB_STATUS_UPD";
                this.body = new Gson().toJson(  new Body(pdbKey, status) );
            }

            @Getter
            public static class Body implements Serializable {
                private final String key;
                private final String status;

                public Body(String pdbKey, String status) {
                    this.key = pdbKey;
                    this.status = status;
                }
            }
        }

        @Getter
        private static class PdbReqJobMessage implements Serializable {
            private final String apiKey;
            private final String clientCode;
            private final String messageCode;
            private final String body;

            public PdbReqJobMessage( String apiKey, String pdbKey ) {
                this.apiKey = apiKey;
                this.clientCode = "PDB";
                this.messageCode = "PDB_REQ_JOB";
                this.body = new Gson().toJson(  new PdbReqJobMessage.Body( pdbKey ) );
            }

            @Getter
            public static class Body implements Serializable {
                private final String key;

                public Body( String pdbKey ) {
                    key = pdbKey;
                }
            }
        }


        @Getter
        private static class PdbResultDepMessage implements Serializable {
            private final String apiKey;
            private final String clientCode;
            private final String messageCode;
            private final String body;

            public PdbResultDepMessage(String apiKey, String pdbKey, Long seed, Boolean success,
                                       Long resultSetHash, String error, String logical, String physical, Long actual) {
                this.apiKey = apiKey;
                this.clientCode = "PDB";
                this.messageCode = "PDB_RESULT_DEP";
                this.body = new Gson().toJson( new Body(pdbKey, seed, resultSetHash, success, error, logical, physical, actual) );
            }

            @Getter
            public static class Body implements Serializable {
                private final String pdbKey;
                private final Long seed;
                private final Long resultSetHash;
                private final Boolean success;
                private final String error;
                private final String logical;
                private final String physical;
                private final Long actual;

                public Body(String pdbKey, Long seed, Long resultSetHash, Boolean success, String error,
                            String logical, String physical, Long actual) {
                    this.pdbKey = pdbKey;
                    this.seed = seed;
                    this.resultSetHash = resultSetHash;
                    this.success = success;
                    this.error = error;
                    this.logical = logical;
                    this.physical = physical;
                    this.actual = actual;
                }
            }
        }


    }

    private static void testRoutine() throws URISyntaxException {
        String apiKey = UUID.randomUUID().toString();
        URI uri = new URI("ws://0.0.0.0:44567/ws");
        try {
            PolyfierClientPctrl polyfierClientPctrl = new PolyfierClientPctrl(uri, apiKey, "master");

            Pair<UUID, Profile> job = polyfierClientPctrl.requestJob();

            log.debug("RECEIVED UUID FOR PDB" + job.getKey().toString() );

            PolyfierClientPdb polyfierClientPdb = new PolyfierClientPdb(uri, apiKey, job.getKey());

            Thread.sleep( 20 );

            Profile profile = polyfierClientPdb.requestJob();

            Random random = new Random(0);
            for (int i = 0; i < 1000; i++) {
                polyfierClientPdb.depositResult(
                        Long.parseLong("" + i),
                        random.nextBoolean(),
                        random.nextLong(),
                        "Error-" + random.nextInt(50),
                        "Plan-Log-" + random.nextInt(100),
                        "Plan-Phy- " + random.nextInt(100),
                        random.nextLong(1000));
                Thread.sleep(20);
            }

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        try {
            log.debug("Waiting...");
            Thread.sleep(120000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


}
