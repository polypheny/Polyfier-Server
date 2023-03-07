package server;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import connect.QueryLogConnection;
import io.javalin.Javalin;
import io.javalin.http.HttpStatus;
import logging.QueueAppender;
import lombok.val;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.filter.ThresholdFilter;
import server.config.SeedsConfig;
import server.generators.BooleanPermuter;
import server.generators.PolyphenyDbProfileGenerator;
import server.requests.PolyphenyControlRequest;
import server.requests.PolyphenyDbRequest;
import server.responses.PolyphenyControlResponse;

import java.io.*;
import java.nio.file.Files;
import java.util.*;

public class Server implements Runnable {

    private static Javalin app;
    private static final Logger LOGGER = LogManager.getRootLogger();
    private static QueueAppender queueAppender;
    private final ServerConfig serverConfig;
    private final QueryLogConnection queryLogConnection;

    private PolyphenyDbProfileGenerator polyphenyDbProfileGenerator;

    private final String phase;
    private final String defcon;

    public Server(ServerConfig serverConfig, QueryLogConnection queryLogConnection ) {
        this.serverConfig = serverConfig;
        this.queryLogConnection = queryLogConnection;
        this.phase = "IDLE";
        this.defcon = "5";

        org.apache.logging.log4j.core.Logger logger = (org.apache.logging.log4j.core.Logger)LogManager.getRootLogger();
        Appender defaultAppender = logger.getAppenders().get("Console");
        queueAppender = QueueAppender.createAppender("QueueAppender", ThresholdFilter.createFilter(Level.INFO, Filter.Result.ACCEPT, Filter.Result.DENY), defaultAppender.getLayout());
        queueAppender.start();
        logger.addAppender( queueAppender );

        app = Javalin.create();
        app.before(ctx -> {
            LOGGER.debug("REQ: [" + ctx.ip() + "] - [" + ctx.req().getMethod() + "] ~" + ctx.req().getPathInfo() );
        });
        app.after(ctx -> {

        });

        // ---- HTML -----
        app.get("/", ctx -> {
            ctx.status(HttpStatus.OK).contentType("text/html").html( getTextFile("web/html/base.html"));
        });
        app.get("/log", ctx -> {
            ctx.status(HttpStatus.OK).contentType("text/html").html( getTextFile("web/html/log.html"));
        });


        // ---- CSS ------
        app.get("/web/css/base.css", ctx -> {
            ctx.status(HttpStatus.OK).contentType("text/css").result( getTextFile("web/css/base.css"));
        });
        app.get("/web/css/log.css", ctx -> {
            ctx.status(HttpStatus.OK).contentType("text/css").result( getTextFile("web/css/log.css"));
        });


        // ---- JS ------
        app.get("/web/js/log.js", ctx -> {
            ctx.status(HttpStatus.OK).contentType("text/javascript").result( getTextFile("web/js/log.js"));
        });
        app.get("/web/js/base.js", ctx -> {
            ctx.status(HttpStatus.OK).contentType("text/javascript").result( getTextFile("web/js/base.js"));
        });


        // ---- IMG -----
        app.get("/web/img/polyphenydb-logo.png", ctx -> {
            OutputStream outputStream = ctx.status(HttpStatus.OK).contentType("image/png").outputStream();
            getFile(outputStream, "web/img/polyphenydb-logo.png");
        });
        app.get("/web/img/log-icon.png", ctx -> {
            OutputStream outputStream = ctx.status(HttpStatus.OK).contentType("image/png").outputStream();
            getFile(outputStream, "web/img/log-icon.png");
        });
        app.get("/web/img/icon.png", ctx -> {
             OutputStream outputStream = ctx.status(HttpStatus.OK).contentType("image/png").outputStream();
             getFile(outputStream, "web/img/icon.png");
        });
        app.get("/web/img/polyfier-schema.png", ctx -> {
            OutputStream outputStream = ctx.status(HttpStatus.OK).contentType("image/png").outputStream();
            getFile(outputStream, "web/img/polyfier-schema.png");
        });


        // ---- Fonts -----
        app.get("/web/fonts/jetbrains_mono/JetBrainsMono-Italic-VariableFont_wght.ttf", ctx -> {
            OutputStream outputStream = ctx.status(HttpStatus.OK).contentType("font/ttf").outputStream();
            getFile(outputStream, "web/fonts/jetbrains_mono/JetBrainsMono-Italic-VariableFont_wght.ttf");
        });


        // ---- Requests ----
        app.get("/request/status-update.json", ctx -> {
            ctx.status(HttpStatus.OK).contentType("text/json").result(getStatusUpdate());
        });

        // ----- Log Updates -----
        app.get("/request/log.json", ctx -> {
            queueAppender.parse().ifPresentOrElse(
                    s -> { ctx.status(HttpStatus.OK).contentType("text/json").result(s); },
                    () -> ctx.status(HttpStatus.NO_CONTENT)
            );
        });
        app.post("/request/log.json", ctx -> {
            String logPost = ctx.body();
        });


        // --"""--"""--"""--"""--"""--"""--"""--"""--"""--"""--"""--"""--"""--"""--"""--"""--"""--"""--"""--"""--"
        //                                      Polypheny Control
        // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

        app.post("/request/polypheny-control/sign-in", ctx -> {
            PolyphenyControlRequest.SignIn request = ctx.bodyValidator( PolyphenyControlRequest.SignIn.class ).get();

            queryLogConnection.logInPolyphenyControlClient(
                    queryLogConnection.getStatement(),
                    request.getApiKey()
            );

            ctx.status( HttpStatus.OK );

        });

        app.post( "/request/polypheny-control/sign-out", ctx -> {
            PolyphenyControlRequest.SignOut request = ctx.bodyValidator( PolyphenyControlRequest.SignOut.class ).get();


        });

        app.post( "/request/polypheny-control/keep-alive", ctx -> {
            PolyphenyControlRequest.KeepAlive request = ctx.bodyValidator( PolyphenyControlRequest.KeepAlive.class ).get();

            queryLogConnection.refreshControlClientStatus(
                    queryLogConnection.getStatement(),
                    request.getApiKey(),
                    request.getStatus()
            );
            ctx.status( HttpStatus.OK );
        });

        app.post("/request/polypheny-control/get-task", ctx -> {
            PolyphenyControlRequest.StartConfiguration request = ctx.bodyValidator( PolyphenyControlRequest.StartConfiguration.class ).get();

            if ( queryLogConnection.controlClientIsRegistered(
                    queryLogConnection.getStatement(),
                    request.getApiKey()
            ) ) {

                if ( queryLogConnection.controlClientIsLoggedIn(
                        queryLogConnection.getStatement(),
                        request.getApiKey()
                ) ) {

                    String polyphenyDbClientId = UUID.randomUUID().toString();

                    SeedsConfig seedsConfig = new SeedsConfig.SeedsBuilder()
                            .addRange( 0, 1000 )
                            .build();

                    PolyphenyDbProfile profile = polyphenyDbProfileGenerator.createProfile(
                            polyphenyDbClientId, request.getApiKey(), seedsConfig );

                    queryLogConnection.registerPolyphenyDbProfile(
                            queryLogConnection.getStatement(),
                            request.getApiKey(),
                            profile
                    );

                    PolyphenyControlResponse.StartConfiguration response = new PolyphenyControlResponse.StartConfiguration(
                            profile.getStartConfig(),
                            polyphenyDbClientId
                    );

                    ctx.header("Content-type", "text/json");
                    ctx.result(
                        new GsonBuilder().create().toJson( response )
                    );

                } else {

                    ctx.status( HttpStatus.FORBIDDEN );
                    return;
                }


                ctx.status( HttpStatus.OK );
                return;
            }

            ctx.status( HttpStatus.FORBIDDEN );

        });

        // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+c



        // --"""--"""--"""--"""--"""--"""--"""--"""--"""--"""--"""--"""--"""--"""--"""--"""--"""--"""--"""--"""--"
        //                                      Polypheny DB
        // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+







        // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+c


        app.post("/request/polypheny-db-task/{db-client_id}/{db-branch}/{db-version}", ctx -> {
            // Request from Polypheny-DB for task.
            // Todo validate
            String clientId = ctx.queryParamAsClass("db-client_id", String.class ).get();
            String branch = ctx.queryParamAsClass("db-branch", String.class ).get();
            String version = ctx.queryParamAsClass("db-version", String.class ).get();
        });

        // ------ Results ------
        app.post("/request/polypheny-control.json", ctx -> {
            /*
            {
                // Todo define...
            }
             */
        });

        app.post("/request/polypheny-db.json", ctx -> {
            /*
            {
                "client": "<id>",
                "logical": "<plan>",
                "physical": "<plan/empty>",
                "message": "<msg/empty>",
                "cause": "<err/empty>",
                "result": "<res/empty>",
                "seed": "<seed>",
                "actual": "<time/empty>",
                "predicted": "<time/empty>",
                "success": "<t/f>"
            }
            */
        });

        // Profile Generation

        HashMap<String, String> dataConfigPreset = new HashMap<>();
        dataConfigPreset.put("dataConfig0", "");
        dataConfigPreset.put("dataConfig1", "");
        dataConfigPreset.put("dataConfig2", "");
        dataConfigPreset.put("dataConfig3", "");

        List<String> dataConfigPermeable = List.of();

        HashMap<String, String> queryConfigPreset = new HashMap<>();
        queryConfigPreset.put("queryConfig0", "");
        queryConfigPreset.put("queryConfig1", "");
        queryConfigPreset.put("queryConfig2", "");
        queryConfigPreset.put("queryConfig3", "");

        List<String> queryConfigPermeable = List.of();

        HashMap<String, String> storeConfigPreset = new HashMap<>();
        storeConfigPreset.put("CASSANDRA", "false");
        storeConfigPreset.put("NEO4J", "false");
        storeConfigPreset.put("COTTONTAIL", "false");

        List<String> storeConfigPermeable = List.of(
                "HSQLDB",
                "POSTGRESQL",
                "MONGODB",
                "MONETDB"
        );

        HashMap<String, String> partConfigPreset = new HashMap<>();
        partConfigPreset.put("partConfig0", "");
        partConfigPreset.put("partConfig1", "");
        partConfigPreset.put("partConfig2", "");
        partConfigPreset.put("partConfig3", "");

        List<String> partConfigPermeable = List.of();

        HashMap<String, String> startConfigPreset = new HashMap<>();
        startConfigPreset.put("startConfig0", "");
        startConfigPreset.put("startConfig1", "");
        startConfigPreset.put("startConfig2", "");
        startConfigPreset.put("startConfig3", "");

        List<String> startConfigPermeable = List.of();


        this.polyphenyDbProfileGenerator = PolyphenyDbProfileGenerator.create(
                dataConfigPermeable,
                dataConfigPreset,
                queryConfigPermeable,
                queryConfigPreset,
                storeConfigPermeable,
                storeConfigPreset,
                partConfigPermeable,
                partConfigPreset,
                startConfigPermeable,
                startConfigPreset,
                true
        );

    }

    private String getStatusUpdate() {
        HashMap<String, String> status = new HashMap<>();
        status.put("server-status", "RUNNING");
        status.put("database-status", (queryLogConnection.isActive()) ? "CONNECTED":"DISCONNECTED");
        try {
            Process process = Runtime.getRuntime().exec("docker container inspect -f '{{.State.Running}}' polypheny-connector");
            BufferedReader stdInput = new BufferedReader(new InputStreamReader(process.getInputStream()));
            status.put("docker-status", (Boolean.parseBoolean(stdInput.readLine().replace("\n", "").replace("'", ""))) ? "CONNECTED":"DISCONNECTED");
        } catch (IOException e) {
            LOGGER.warn( "Can't connect to docker: " + e.getMessage() );
            status.put("docker-status", "DISCONNECTED");
        }
        status.put("polyfier-phase", this.phase);

        // Other Status Checks
        //...
        status.put("polypheny-db", "UNKNOWN");
        status.put("polypheny-control", "UNKNOWN");
        status.put("defcon", this.defcon);

        Gson gson = new Gson();

        return gson.toJson(status);
    }

    private String getTextFile( String path ) {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(
                    Objects.requireNonNull(
                            this.getClass().getClassLoader().getResource( path )
                    ).openStream()
            ));
            StringBuilder sb = new StringBuilder();
            while ( reader.ready() ) {
                sb.append( reader.readLine() ).append("\n");
            }
            return sb.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void getFile( OutputStream outputStream, String path ) {
        try {
            File file = new File(Objects.requireNonNull(this.getClass().getClassLoader().getResource(path)).getFile());
            Files.copy(file.toPath(), outputStream);
            outputStream.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void run() {
        getStatusUpdate();
        app.start( serverConfig.getHost(), serverConfig.getPort() );
    }
}
