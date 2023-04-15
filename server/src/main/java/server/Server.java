package server;

import io.javalin.Javalin;
import lombok.extern.slf4j.Slf4j;
import server.messages.ClientMessage;
import server.messages.ServerMessage;

import java.util.Objects;

@Slf4j
public class Server {
    private static Javalin app;

    public Server( ServerConfig serverConfig ) {
        setUpAPI();
        app.start( serverConfig.getHost(), serverConfig.getPort() );
    }

    private void setUpAPI() {
        app = Javalin.create();

        app.before( ctx -> log.debug("REQ: [" + ctx.ip() + "] - [" + ctx.req().getMethod() + "] ~" + ctx.req().getPathInfo() + "\t" + ctx.body() ) );
        app.after( ctx -> {} ); //

        // HTML
        app.get("/", ctx -> ServerMessage.handleStringResponse( ctx, "text/html", "web/html/base.html"));
        app.get("/log", ctx -> ServerMessage.handleStringResponse( ctx, "text/html", "web/html/log.html"));
        app.get("/sys", ctx -> ServerMessage.handleStringResponse( ctx, "text/html", "web/html/sys.html"));

        // CSS
        app.get("/web/css/base.css", ctx -> ServerMessage.handleStringResponse( ctx, "text/css", "web/css/base.css"));
        app.get("/web/css/log.css", ctx -> ServerMessage.handleStringResponse( ctx, "text/css", "web/css/log.css"));
        app.get("/web/css/sys.css", ctx -> ServerMessage.handleStringResponse( ctx, "text/css", "web/css/sys.css"));

        // JavaScript
        app.get("/web/js/log.js", ctx -> ServerMessage.handleStringResponse( ctx, "text/javascript", "web/js/log.js"));
        app.get("/web/js/base.js", ctx -> ServerMessage.handleStringResponse( ctx, "text/javascript","web/js/base.js"));
        app.get("/web/js/sys.js", ctx -> ServerMessage.handleStringResponse( ctx, "text/javascript","web/js/sys.js"));

        // Other Files
        app.get("/web/img/polyphenydb-logo.png", ctx -> ServerMessage.handleMediaResponse( ctx, "image/png", "web/img/polyphenydb-logo.png" ));
        app.get("/web/img/log-icon.png", ctx -> ServerMessage.handleMediaResponse( ctx, "image/png", "web/img/log-icon.png" ));
        app.get("/web/img/icon.png", ctx -> ServerMessage.handleMediaResponse( ctx, "image/png", "web/img/icon.png" ));
        app.get("/web/img/polyfier-schema.png", ctx -> ServerMessage.handleMediaResponse( ctx, "image/png", "web/img/polyfier-schema.png" ));
        app.get("/web/fonts/jetbrains_mono/JetBrainsMono-Italic-VariableFont_wght.ttf", ctx -> ServerMessage.handleMediaResponse( ctx, "font/ttf", "web/fonts/jetbrains_mono/JetBrainsMono-Italic-VariableFont_wght.ttf" ));

        // API WebSocket
        app.ws("/ws", ws -> {
            ws.onConnect( session  -> {
                log.info("WebSocket connected /ws" + session.getSessionId());
            });
            ws.onClose( session  -> {
                log.info("WebSocket closed /ws:" + session.getSessionId() + " StatusCode: " + session.status() + " Reason: " + session.reason() );
                ClientMessage.handleConnectionLoss( session );
            });
            ws.onError( session  -> {
                log.info("WebSocket error for /ws: " + session.getSessionId());
                Objects.requireNonNull(session.error()).printStackTrace();
            });
            // API Message Processing
            ws.onMessage( session -> {
                log.info("WebSocket Message for /ws: " + session.message() );
                try {
                    ClientMessage.processMessage( session );
                } catch (Exception e) {
                    log.error("Error:", e);
                    throw new RuntimeException(e);
                }
            } );
        });


    }


}
