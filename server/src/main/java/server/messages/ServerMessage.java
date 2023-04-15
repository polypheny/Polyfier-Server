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
import connect.QueryLogConnection;
import io.javalin.http.Context;
import io.javalin.http.HttpStatus;
import io.javalin.websocket.WsContext;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import server.ServerConfig;
import logging.WebSocketAppender;
import server.clients.Browser;
import server.clients.PCtrl;
import server.clients.PDB;
import server.profile.SeedsConfig;
import server.profile.Profile;
import server.profile.generators.ProfileGenerator;
import server.requests.Requests;

import java.io.*;
import java.nio.file.Files;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public abstract class ServerMessage {
    @Getter
    @Setter
    private static ServerConfig serverConfig;
    @Getter
    @Setter
    private static ProfileGenerator profileGenerator;

    private static QueryLogConnection queryLogConnection;

    @Getter
    private static final Object REGISTER_LOCK = new Object();

    public static void configureServerMessage(
            ProfileGenerator initialProfileGenerator,
            ServerConfig initialServerConfig
    ) {
        serverConfig = initialServerConfig;
        profileGenerator = initialProfileGenerator;
        try {
            queryLogConnection = QueryLogConnection.with( serverConfig.getUrl(), serverConfig.getUser(), serverConfig.getPassword() );
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        Thread browserStatusThread = new Thread( ServerMessage::runBrowserRoutine );
        browserStatusThread.setDaemon( true );
        browserStatusThread.start();
    }

    private static final HashMap<String, PDB> PDB_CLIENTS = new HashMap<>();
    private static final HashMap<String, PCtrl> PCTRL_CLIENTS = new HashMap<>();
    private static final HashMap<String, Browser> BROWSER_SUBS = new HashMap<>();
    private static final HashMap<String, String> PDB_JOBS = new HashMap<>();

    public static void subscribeLog( WsContext wsContext ) {
        WebSocketAppender.addSession( wsContext );
    }

    public static void unsubscribeLog( WsContext wsContext ) {
        WebSocketAppender.removeSession( wsContext );
    }

    public static void subscribeSys( WsContext wsContext ) {
        BROWSER_SUBS.put( wsContext.getSessionId(), new Browser( wsContext ) );
    }

    public static void unsubscribeSys( WsContext wsContext ) {
        BROWSER_SUBS.remove( wsContext.getSessionId() );
    }

    public static void runBrowserRoutine() {
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor( Thread::new );
        final Gson gson = new Gson();
        executorService.scheduleAtFixedRate( () -> {
            if ( ! BROWSER_SUBS.isEmpty() ) {
                String statusMessage = gson.toJson( statusResponse() );
                BROWSER_SUBS.values().forEach( browser -> {
                    browser.getWsContext().send( statusMessage );
                });
            }
        }, 5, 5, TimeUnit.SECONDS);
    }

    synchronized public static void handlePCTRLSignIn( ClientMessage clientMessage, String pctrlKey ) {
        PCtrl pctrl = new PCtrl(
                clientMessage.getWsContext(),
                pctrlKey,
                clientMessage.getWsContext().getSessionId(),
                clientMessage.getApiKey(),
                clientMessage.getBranch(),
                "IDLE",
                true,
                System.currentTimeMillis(),
                System.currentTimeMillis()
        );

        try {
            queryLogConnection.registerPctrl( pctrl );
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        PCTRL_CLIENTS.put( pctrlKey, pctrl );
        respondOk( clientMessage.getWsContext() );
    }

    public static void handlePCTRLSignOut( ClientMessage clientMessage, String pctrlKey  ) {
        PCtrl pctrl = PCTRL_CLIENTS.get( pctrlKey );

        if (!Objects.equals(pctrl.getSessionId(), clientMessage.getWsContext().getSessionId())) {
            pctrl.setSessionId( clientMessage.getWsContext().getSessionId() );
            pctrl.setWsContext( clientMessage.getWsContext() );
        }

        pctrl.setActive( false );
        pctrl.setUpdateTime( System.currentTimeMillis() );

        PCTRL_CLIENTS.remove( clientMessage.getWsContext().getSessionId() );
        respondOk( clientMessage.getWsContext() );
    }

    public static void handlePCTRLJob( ClientMessage clientMessage, String pctrlKey, String pdbKey ) {
        synchronized ( PDB_CLIENTS ) {
            Gson gson = new Gson();

            PCtrl pctrl = PCTRL_CLIENTS.get( pctrlKey );
            if (!Objects.equals(pctrl.getSessionId(), clientMessage.getWsContext().getSessionId())) {
                pctrl.setSessionId( clientMessage.getWsContext().getSessionId() );
                pctrl.setWsContext( clientMessage.getWsContext() );
            }
            pctrl.setUpdateTime( System.currentTimeMillis() );

            Profile profile = profileGenerator.createProfile( new SeedsConfig.SeedsBuilder().addRange( 0, 1000 ).build() ); // Todo

            String job = gson.toJson( profile );

            PDB pdb = new PDB(
                    null,
                    0,
                    0.0,
                    profile.getStoreConfig().getStores(),
                    pdbKey,
                    pctrlKey,
                    null,
                    profile.getStartConfig().getParameters().get("Branch"),
                    "ORDERED",
                    false,
                    System.currentTimeMillis(),
                    System.currentTimeMillis()
            );

            try {
                queryLogConnection.registerPdb( pdb, profile );
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

            log.debug("RECEIVED UUID FOR PDB1 " + pdbKey );
            PDB_JOBS.put( pdbKey, job );
            PDB_CLIENTS.put( pdbKey, pdb );

            clientMessage.getWsContext().send( job );

            clientMessage.getWsContext().send( new Gson().toJson( new ResponseMessage(
                    ResponseMessageCode.JOB.name(), job
            ) ) );
        }
    }

    public static void handlePCTRLStatus( ClientMessage clientMessage, String pctrlKey, String status ) {
        PCtrl pctrl = PCTRL_CLIENTS.get( pctrlKey );

        if (!Objects.equals(pctrl.getSessionId(), clientMessage.getWsContext().getSessionId())) {
            pctrl.setSessionId( clientMessage.getWsContext().getSessionId() );
            pctrl.setWsContext( clientMessage.getWsContext() );
        }

        pctrl.setStatus( status );
        pctrl.setUpdateTime( System.currentTimeMillis() );

        respondOk( clientMessage.getWsContext() );
    }

    public static void handlePDBSignIn( ClientMessage clientMessage, String pdbKey ) {
        synchronized ( PDB_CLIENTS ) {
            PDB pdb = PDB_CLIENTS.get( pdbKey );

            if (!Objects.equals(pdb.getSessionId(), clientMessage.getWsContext().getSessionId())) {
                pdb.setSessionId( clientMessage.getWsContext().getSessionId() );
                pdb.setWsContext( clientMessage.getWsContext() );
            }

            pdb.setActive( true );
            pdb.setStatus("IDLE");
            pdb.setUpdateTime( System.currentTimeMillis() );
            pdb.setWsContext( clientMessage.getWsContext() );
            pdb.setSessionId( clientMessage.getWsContext().getSessionId() );

            respondOk( clientMessage.getWsContext() );

        }
    }

    public static void handlePDBSignOut( ClientMessage clientMessage, String pdbKey ) {
        PDB pdb = PDB_CLIENTS.get( pdbKey );

        log.debug("RECEIVED UUID FOR PDB2 " + pdbKey );

        if (!Objects.equals(pdb.getSessionId(), clientMessage.getWsContext().getSessionId())) {
            pdb.setSessionId( clientMessage.getWsContext().getSessionId() );
            pdb.setWsContext( clientMessage.getWsContext() );
        }

        pdb.setActive( false );
        pdb.setUpdateTime( System.currentTimeMillis() );

        PDB_CLIENTS.remove( pdbKey );
        PDB_JOBS.remove( pdbKey );
        respondOk( clientMessage.getWsContext() );
    }

    public static void handlePDBStatus(ClientMessage clientMessage, String pdbKey, String status ) {
        PDB pdb = PDB_CLIENTS.get( pdbKey );

        if (!Objects.equals(pdb.getSessionId(), clientMessage.getWsContext().getSessionId())) {
            pdb.setSessionId( clientMessage.getWsContext().getSessionId() );
            pdb.setWsContext( clientMessage.getWsContext() );
        }

        pdb.setStatus( status );
        pdb.setUpdateTime( System.currentTimeMillis() );

        respondOk( clientMessage.getWsContext() );
    }

    public static void handlePDBJob( ClientMessage clientMessage, String pdbKey ) {
        PDB pdb = PDB_CLIENTS.get( pdbKey );

        if (!Objects.equals(pdb.getSessionId(), clientMessage.getWsContext().getSessionId())) {
            pdb.setSessionId( clientMessage.getWsContext().getSessionId() );
            pdb.setWsContext( clientMessage.getWsContext() );
        }

        pdb.setUpdateTime( System.currentTimeMillis() );
        clientMessage.getWsContext().send( new Gson().toJson( new ResponseMessage(
                ResponseMessageCode.JOB.name(),
                PDB_JOBS.get( pdbKey )
        ) ) );
    }

    public static void handlePDBResult( ClientMessage clientMessage, ClientMessage.PDBResult result, Integer size ) {
        PDB pdb = PDB_CLIENTS.get( result.getPdbKey() );

        if (!Objects.equals(pdb.getSessionId(), clientMessage.getWsContext().getSessionId())) {
            pdb.setSessionId( clientMessage.getWsContext().getSessionId() );
            pdb.setWsContext( clientMessage.getWsContext() );
        }

        try {
            queryLogConnection.insertResult( pdb, result );
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        pdb.addDataCount( size * 0.001d );
        pdb.incrementResultCount();
        pdb.setUpdateTime( System.currentTimeMillis() );


        respondOk( clientMessage.getWsContext() );
    }

    public static BrowserStatusResponse statusResponse() {
        return new BrowserStatusResponse(
                System.currentTimeMillis(),
                PDB_CLIENTS.values().toArray(),
                PCTRL_CLIENTS.values().toArray()
        );
    }


    private record BrowserStatusResponse( Long time, Object[] pdbClients, Object[] pctrlClients) implements Serializable {}

    private static void respondOk( WsContext wsContext ) {
        wsContext.send( new Gson().toJson( new ResponseMessage( ResponseMessageCode.OK.name(), "" ) ) );
    }

    public static void handleStringResponse(Context ctx, String contentType, String path ) {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(
                    Objects.requireNonNull(
                            Requests.class.getClassLoader().getResource( path )
                    ).openStream()
            ));
            StringBuilder sb = new StringBuilder();
            while ( reader.ready() ) {
                sb.append( reader.readLine() ).append("\n");
            }
            ctx.status(HttpStatus.OK).contentType( contentType ).result( sb.toString() );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void handleMediaResponse( Context ctx, String contentType, String path ) {
        OutputStream outputStream = ctx.status(HttpStatus.OK).contentType( contentType ).outputStream();
        try {
            File file = new File(Objects.requireNonNull( Requests.class.getClassLoader().getResource(path)).getFile());
            Files.copy(file.toPath(), outputStream);
            outputStream.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Getter
    @AllArgsConstructor
    private enum ResponseMessageCode {
        OK( null ),
        JOB( Profile.class );

        private final Class<?> clazz;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    private static class ResponseMessage implements Serializable {
        private String messageCode;
        private String body;
    }

}
