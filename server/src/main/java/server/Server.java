package server;


import com.google.gson.Gson;
import connect.QueryLogConnection;
import io.javalin.Javalin;
import io.javalin.http.HttpStatus;
import logging.QueueAppender;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.filter.ThresholdFilter;

import java.io.*;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Objects;

public class Server implements Runnable {

    private static Javalin app;
    private static final Logger LOGGER = LogManager.getRootLogger();
    private static QueueAppender queueAppender;
    private final ServerConfig serverConfig;
    private final QueryLogConnection queryLogConnection;

    private String phase;
    private String defcon;

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
            /*
            For UI....
            {
                "client": "<id>",
                "logs": {
                    "0": "<log-message>",
                    "1": "<log-message>",
                    "2": "<log-message>"
                    <...>
                }
            }
             */
            queueAppender.parse().ifPresentOrElse(
                    s -> { ctx.status(HttpStatus.OK).contentType("text/json").result(s); },
                    () -> ctx.status(HttpStatus.NO_CONTENT)
            );
        });
        app.post("/request/log.json", ctx -> {
            String logPost = ctx.body();
            /*
            {
                "client": "<id>",
                "logs": {
                    "0": "<log-message>",
                    "1": "<log-message>",
                    "2": "<log-message>"
                    <...>
                }
            }
             */
        });

        // ------ Jobs -------
        app.get("/request/polypheny-control.json", ctx -> {
            /*
            {
                "timestamp": "<time>"
                "seed": "<seed>",
                "queries": "<int>",
                "queryType": "<DML/DQL>"
                "dataStores": {
                    "<ds>": "<t/f>",
                    "<ds>": "<t/f>",
                    "<ds>": "<t/f>",
                    "<ds>": "<t/f>",
                    ...
                },
                "configurations": {
                    "<config>": "<t/f>",
                    "<config>": "<double>",
                    "<config>": "<int>",
                    ...
                }
            }
            */

        });
        app.get("/request/polypheny-db.json", ctx -> {
            /*
            {
                // Todo define...
            }
             */
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
