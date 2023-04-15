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

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Deprecated
public abstract class Requests {
//    @Getter
//    @Setter
//    private static ServerConfig serverConfig;
//    @Getter
//    @Setter
//    private static ProfileGenerator profileGenerator;
//

//
//    public static void handlePolyphenyControlSignIn( Context ctx ) throws SQLException {
//        PolyphenyControlRequest.SignIn request;
//        try {
//            request = new GsonBuilder().create().fromJson( ctx.body(), PolyphenyControlRequest.SignIn.class );
//
//            QueryLogConnection queryLogConnection = QueryLogConnection.with( serverConfig.getUrl(), serverConfig.getUser(), serverConfig.getPassword() );
//            queryLogConnection.singInPctrl( request.getApiKey() );
//            queryLogConnection.close();
//
//        } catch ( JsonSyntaxException jsonSyntaxException ) {
//            log.error( "", jsonSyntaxException );
//            ctx.status( HttpStatus.BAD_REQUEST ).result("Invalid body content for sign-in request: " + ctx.body() );
//            return;
//        }
//        ctx.status( HttpStatus.OK );
//    }
//
//    public static void handlePolyphenyControlSignOut( Context ctx ) throws SQLException {
//        PolyphenyControlRequest.SignOut request;
//        try {
//            request = new GsonBuilder().create().fromJson( ctx.body(), PolyphenyControlRequest.SignOut.class );
//
//            QueryLogConnection queryLogConnection = QueryLogConnection.with( serverConfig.getUrl(), serverConfig.getUser(), serverConfig.getPassword() );
//            queryLogConnection.signOutCtrl( request.getApiKey() );
//            queryLogConnection.close();
//
//        } catch ( JsonSyntaxException jsonSyntaxException ) {
//            log.error( "", jsonSyntaxException );
//            ctx.status( HttpStatus.BAD_REQUEST ).result("Invalid body content for sign-out request: " + ctx.body() );
//            return;
//        }
//        ctx.status( HttpStatus.OK );
//    }
//
//    public static void handlePolyphenyControlKeepAlive( Context ctx ) throws SQLException {
//        PolyphenyControlRequest.KeepAlive request;
//        try {
//            request = new GsonBuilder().create().fromJson( ctx.body(), PolyphenyControlRequest.KeepAlive.class );
//
//            QueryLogConnection queryLogConnection =  QueryLogConnection.with( serverConfig.getUrl(), serverConfig.getUser(), serverConfig.getPassword() );
//            queryLogConnection.refreshCtrlStatus( request.getApiKey(), request.getStatus() );
//            queryLogConnection.close();
//
//        } catch ( JsonSyntaxException jsonSyntaxException ) {
//            log.error( "", jsonSyntaxException );
//            ctx.status( HttpStatus.BAD_REQUEST ).result("Invalid body content for keep-alive request: " + ctx.body() );
//            log.error( "JsonSyntax:", jsonSyntaxException );
//            return;
//        }
//        ctx.status( HttpStatus.OK );
//    }
//
//    public static void handlePolyphenyControlJob( Context ctx ) throws SQLException {
//        PolyphenyControlRequest.StartConfiguration request;
//        try {
//            request = new GsonBuilder().create().fromJson( ctx.body(), PolyphenyControlRequest.StartConfiguration.class );
//
//            // Todo add UI options for this
//            SeedsConfig seedsConfig = new SeedsConfig.SeedsBuilder().addRange( 0, 10000 ).build();
//            //
//            Profile profile = profileGenerator.createProfile( request.getApiKey(), seedsConfig );
//
//            StartConfig startConfig = profile.getStartConfig();
//
//            QueryLogConnection queryLogConnection =  QueryLogConnection.with( serverConfig.getUrl(), serverConfig.getUser(), serverConfig.getPassword() );
//            String order_key = queryLogConnection.issueOrder( request.getApiKey(), profile );
//            queryLogConnection.close();
//
//            PolyphenyControlResponse.StartConfiguration response = new PolyphenyControlResponse.StartConfiguration(
//                    startConfig,
//                    order_key
//            );
//
//            ctx.status( HttpStatus.OK ).result( new Gson().toJson( response ) );
//
//        } catch ( JsonSyntaxException jsonSyntaxException ) {
//            log.error( "", jsonSyntaxException );
//            ctx.status( HttpStatus.BAD_REQUEST ).result("Invalid body content for job request: " + ctx.body() );
//            return;
//        }
//    }
//
//    public static void handlePolyphenyDbJob( Context ctx ) throws SQLException {
//        PolyphenyDbRequest.Job request;
//        try {
//            request = new GsonBuilder().create().fromJson( ctx.body(), PolyphenyDbRequest.Job.class );
//
//            QueryLogConnection queryLogConnection = QueryLogConnection.with( serverConfig.getUrl(), serverConfig.getUser(), serverConfig.getPassword() );
//            Profile profile = queryLogConnection.buildCompositeProfile( request.getOrderKey() );
//            queryLogConnection.close();
//
//            PolyphenyDbResponse.Job response = new PolyphenyDbResponse.Job(
//                    profile.getSchemaConfig(),
//                    profile.getDataConfig(),
//                    profile.getQueryConfig(),
//                    profile.getStoreConfig(),
//                    profile.getPartitionConfig(),
//                    profile.getIssuedSeeds()
//            );
//
//            ctx.status( HttpStatus.OK ).result( new Gson().toJson( response ) );
//
//        } catch ( JsonSyntaxException jsonSyntaxException ) {
//            log.error( "", jsonSyntaxException );
//            ctx.status( HttpStatus.BAD_REQUEST ).result("Invalid body content for job request: " + ctx.body() );
//            return;
//        }
//    }
//
//    public static void handlePolyphenyDbResult( Context ctx ) throws SQLException {
//        PolyphenyDbRequest.Result request;
//        try {
//            request = new GsonBuilder().create().fromJson( ctx.body(), PolyphenyDbRequest.Result.class );
//
//            QueryLogConnection queryLogConnection = QueryLogConnection.with( serverConfig.getUrl(), serverConfig.getUser(), serverConfig.getPassword() );
//            queryLogConnection.insertResult(
//                    request.getOrderKey(),
//                    request.getSeed(),
//                    request.getSuccess(),
//                    request.getError(),
//                    request.getResultSetHash(),
//                    request.getLogical(),
//                    request.getPhysical(),
//                    request.getActual(),
//                    request.getPredicted()
//            );
//            queryLogConnection.close();
//
//        } catch ( JsonSyntaxException jsonSyntaxException ) {
//            log.error( "", jsonSyntaxException );
//            ctx.status( HttpStatus.BAD_REQUEST ).result("Invalid body content for result request: " + ctx.body() );
//            return;
//        }
//
//        ctx.status( HttpStatus.OK );
//
//    }
//
//    public static void handleStatusRequest(Context context) {
//
//
//
//
//
//
//
//    }

}
