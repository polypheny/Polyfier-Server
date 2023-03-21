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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import connect.QueryLogConnection;
import io.javalin.http.Context;
import io.javalin.http.HttpStatus;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import server.config.StartConfig;
import server.generators.Profile;
import server.config.SeedsConfig;
import server.generators.ProfileGenerator;

import java.io.*;
import java.nio.file.Files;
import java.sql.SQLException;
import java.util.Objects;

@Slf4j
public abstract class Requests {
    @Getter
    @Setter
    private static QueryLogConnection queryLogConnection;
    @Getter
    @Setter
    private static ProfileGenerator profileGenerator;

    public static void handleStringResponse( Context ctx, String contentType, String path ) {
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

    public static void handlePolyphenyControlSignIn( Context ctx ) throws SQLException {
        PolyphenyControlRequest.SignIn request;
        try {
            request = new GsonBuilder().create().fromJson( ctx.body(), PolyphenyControlRequest.SignIn.class );
            queryLogConnection.signInCtrl( request.getApiKey() );
        } catch ( JsonSyntaxException jsonSyntaxException ) {
            log.error( "", jsonSyntaxException );
            ctx.status( HttpStatus.BAD_REQUEST ).result("Invalid body content for sign-in request: " + ctx.body() );
            return;
        }
        ctx.status( HttpStatus.OK );
    }

    public static void handlePolyphenyControlSignOut( Context ctx ) throws SQLException {
        PolyphenyControlRequest.SignOut request;
        try {
            request = new GsonBuilder().create().fromJson( ctx.body(), PolyphenyControlRequest.SignOut.class );
            queryLogConnection.signOutCtrl( request.getApiKey() );
        } catch ( JsonSyntaxException jsonSyntaxException ) {
            log.error( "", jsonSyntaxException );
            ctx.status( HttpStatus.BAD_REQUEST ).result("Invalid body content for sign-out request: " + ctx.body() );
            return;
        }
        ctx.status( HttpStatus.OK );
    }

    public static void handlePolyphenyControlKeepAlive( Context ctx ) throws SQLException {
        PolyphenyControlRequest.KeepAlive request;
        try {
            request = new GsonBuilder().create().fromJson( ctx.body(), PolyphenyControlRequest.KeepAlive.class );
            queryLogConnection.refreshCtrlStatus( request.getApiKey(), request.getStatus() );
        } catch ( JsonSyntaxException jsonSyntaxException ) {
            log.error( "", jsonSyntaxException );
            ctx.status( HttpStatus.BAD_REQUEST ).result("Invalid body content for keep-alive request: " + ctx.body() );
            return;
        }
        ctx.status( HttpStatus.OK );
    }

    public static void handlePolyphenyControlJob(Context ctx ) throws SQLException {
        PolyphenyControlRequest.StartConfiguration request;
        try {
            request = new GsonBuilder().create().fromJson( ctx.body(), PolyphenyControlRequest.StartConfiguration.class );

            // Todo add UI options for this
            SeedsConfig seedsConfig = new SeedsConfig.SeedsBuilder().addRange( 0, 10000 ).build();
            //
            Profile profile = profileGenerator.createProfile( request.getApiKey(), seedsConfig );

            StartConfig startConfig = profile.getStartConfig();
            String order_key = queryLogConnection.issueOrder( request.getApiKey(), profile );

            PolyphenyControlResponse.StartConfiguration response = new PolyphenyControlResponse.StartConfiguration(
                    startConfig,
                    order_key
            );

            ctx.status( HttpStatus.OK ).result( new Gson().toJson( response ) );

        } catch ( JsonSyntaxException jsonSyntaxException ) {
            log.error( "", jsonSyntaxException );
            ctx.status( HttpStatus.BAD_REQUEST ).result("Invalid body content for job request: " + ctx.body() );
            return;
        }
    }

    public static void handlePolyphenyDbJob(Context ctx ) throws SQLException {
        PolyphenyDbRequest.Job request;
        try {
            request = new GsonBuilder().create().fromJson( ctx.body(), PolyphenyDbRequest.Job.class );

            Profile profile = queryLogConnection.buildCompositeProfile( request.getOrderKey() );

            PolyphenyDbResponse.Job response = new PolyphenyDbResponse.Job(
                    profile.getSchemaConfig(),
                    profile.getDataConfig(),
                    profile.getQueryConfig(),
                    profile.getStoreConfig(),
                    profile.getPartitionConfig()
            );

            ctx.status( HttpStatus.OK ).result( new Gson().toJson( response ) );

        } catch ( JsonSyntaxException jsonSyntaxException ) {
            log.error( "", jsonSyntaxException );
            ctx.status( HttpStatus.BAD_REQUEST ).result("Invalid body content for job request: " + ctx.body() );
            return;
        }
    }

    public static void handlePolyphenyDbResult(Context ctx ) throws SQLException {
        PolyphenyDbRequest.Result request;
        try {
            request = new GsonBuilder().create().fromJson( ctx.body(), PolyphenyDbRequest.Result.class );

            queryLogConnection.insertResult(
                    request.getOrderKey(),
                    request.getSeed(),
                    request.getSuccess(),
                    request.getError(),
                    request.getResultSetHash(),
                    request.getLogical(),
                    request.getPhysical(),
                    request.getActual(),
                    request.getPredicted()
            );

        } catch ( JsonSyntaxException jsonSyntaxException ) {
            log.error( "", jsonSyntaxException );
            ctx.status( HttpStatus.BAD_REQUEST ).result("Invalid body content for result request: " + ctx.body() );
            return;
        }

        ctx.status( HttpStatus.OK );

    }

}
