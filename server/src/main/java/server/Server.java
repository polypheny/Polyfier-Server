package server;

import connect.QueryLogConnection;
import io.javalin.Javalin;
import lombok.extern.slf4j.Slf4j;
import server.generators.ProfileGenerator;
import server.requests.Requests;

@Slf4j
public class Server {
    private static Javalin app;

    public Server( ServerConfig serverConfig, QueryLogConnection queryLogConnection ) {
        Requests.setQueryLogConnection( queryLogConnection );
        Requests.setProfileGenerator( ProfileGenerator.getProfileGenerator() );

        setUpAPI();
        app.start( serverConfig.getHost(), serverConfig.getPort() );
    }

    private void setUpAPI() {
        app = Javalin.create();

        app.before( ctx -> log.debug("REQ: [" + ctx.ip() + "] - [" + ctx.req().getMethod() + "] ~" + ctx.req().getPathInfo() + "\t" + ctx.body() ) );
        app.after( ctx -> {} ); //

        app.get("/", ctx -> Requests.handleStringResponse( ctx, "text/html", "web/html/base.html"));
        app.get("/log", ctx -> Requests.handleStringResponse( ctx, "text/html", "web/html/log.html"));
        app.get("/web/css/base.css", ctx -> Requests.handleStringResponse( ctx, "text/css", "web/css/base.css"));
        app.get("/web/css/log.css", ctx -> Requests.handleStringResponse( ctx, "text/css", "web/css/log.css"));
        app.get("/web/js/log.js", ctx -> Requests.handleStringResponse( ctx, "text/javascript", "web/js/log.js"));
        app.get("/web/js/base.js", ctx -> Requests.handleStringResponse( ctx, "text/javascript","web/js/base.js"));

        app.get("/web/img/polyphenydb-logo.png", ctx -> Requests.handleMediaResponse( ctx, "image/png", "web/img/polyphenydb-logo.png" ));
        app.get("/web/img/log-icon.png", ctx -> Requests.handleMediaResponse( ctx, "image/png", "web/img/log-icon.png" ));
        app.get("/web/img/icon.png", ctx -> Requests.handleMediaResponse( ctx, "image/png", "web/img/icon.png" ));
        app.get("/web/img/polyfier-schema.png", ctx -> Requests.handleMediaResponse( ctx, "image/png", "web/img/polyfier-schema.png" ));
        app.get("/web/fonts/jetbrains_mono/JetBrainsMono-Italic-VariableFont_wght.ttf", ctx -> Requests.handleMediaResponse( ctx, "font/ttf", "web/fonts/jetbrains_mono/JetBrainsMono-Italic-VariableFont_wght.ttf" ));

        //app.get("/request/browser/status-update.json", Requests::handleStatusRequest );

        app.post("/request/polypheny-control/sign-in", Requests::handlePolyphenyControlSignIn );
        app.post( "/request/polypheny-control/sign-out", Requests::handlePolyphenyControlSignOut );
        app.post( "/request/polypheny-control/keep-alive", Requests::handlePolyphenyControlKeepAlive );
        app.post("/request/polypheny-control/get-task", Requests::handlePolyphenyControlJob);


        app.post("/request/polypheny-db/get-task", Requests::handlePolyphenyDbJob);
        app.post("/request/polypheny-db/result", Requests::handlePolyphenyDbResult);
    }


}
