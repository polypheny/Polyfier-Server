import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import connect.QueryLogConnection;
import logging.WebSocketAppender;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import server.Server;
import server.ServerConfig;
import server.profile.generators.ProfileGenerator;
import server.messages.ServerMessage;

import java.util.Arrays;

@Slf4j
public class PolyfierServer {
    private static final CommandLineParser clParser;
    private static final Options cliOptions;

    static {
        // Logging  ---------------------------------------------------
        // This is the only option to add a custom appender in this context - it will not get recognised whatsoever if
        // added via the log4j2.xml configuration, however it is done. This appender sends logs to browser clients.
        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        Configuration config = ctx.getConfiguration();
        WebSocketAppender webSocketAppender = WebSocketAppender.createAppender("WebSocket");
        webSocketAppender.start();
        config.addAppender(webSocketAppender);
        ctx.getRootLogger().addAppender(webSocketAppender);
        ctx.updateLoggers();
        // Command-Line Options ---------------------------------------
        // Documentation:
        // https://javadoc.io/doc/commons-cli/commons-cli/latest/index.html
        cliOptions = new Options();
        OptionGroup mutuallyExclusiveOptions = new OptionGroup();
        mutuallyExclusiveOptions.addOption(Option.builder()
                    .optionalArg(true)
                    .hasArg(true)
                    .numberOfArgs(2)
                    .valueSeparator(' ')
                    .argName("ip")
                    .argName("port")
                    .option("r")
                    .longOpt("run")
                    .desc("Runs the server.")
                    .build()
        );

        mutuallyExclusiveOptions.setRequired(true);
        cliOptions.addOptionGroup(mutuallyExclusiveOptions);
        clParser = DefaultParser.builder().build();
    }

    public static void runServer( String host, int port ) {
        ServerConfig serverConfig = ServerConfig.fetch();
        serverConfig.setAddress( host, port );
        runServer();
    }

    private static void runServer() {
        log.info("Loading Configurations...");
        ServerConfig serverConfig = ServerConfig.fetch();
        Gson gson = new GsonBuilder().setPrettyPrinting().create();

        if ( ! serverConfig.hasAddress() ) {
            serverConfig.setAddress( "0.0.0.0", 44567 );
        }
        if ( ! serverConfig.hasUrl() ) {
            serverConfig.setUrl( "jdbc:polypheny://localhost:20591/" );
        }
        if ( ! serverConfig.hasCredentials() ) {
            serverConfig.setCredentials("pa", "");
        }
        log.debug("Configuration:");
        log.debug(gson.toJson( serverConfig ));
        log.info("Connecting to PolyphenyDB Backend...");

        ServerMessage.configureServerMessage(
                ProfileGenerator.getProfileGenerator(),
                serverConfig
        );

        QueryLogConnection.initialize( serverConfig.getUrl(), serverConfig.getUser(), serverConfig.getPassword() );
        log.info("Connection established.");
        runServer( serverConfig );
    }


    private static void runServer( ServerConfig serverConfig ) {
        log.info("Running Server...");
        Thread server = new Thread( () -> new Server( serverConfig ) );
        server.start();
        displayBanner( serverConfig );
        try {
            server.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static void displayBanner( ServerConfig serverConfig ) {
        String banner =
                """
                          ___         ___                                     ___                     ___           ___    \s
                         /  /\\       /  /\\                        ___        /  /\\      ___          /  /\\         /  /\\   \s
                        /  /::\\     /  /::\\                      /__/|      /  /:/_    /  /\\        /  /:/_       /  /::\\  \s
                       /  /:/\\:\\   /  /:/\\:\\    ___     ___     |  |:|     /  /:/ /\\  /  /:/       /  /:/ /\\     /  /:/\\:\\ \s
                      /  /:/~/:/  /  /:/  \\:\\  /__/\\   /  /\\    |  |:|    /  /:/ /:/ /__/::\\      /  /:/ /:/_   /  /:/~/:/ \s
                     /__/:/ /:/  /__/:/ \\__\\:\\ \\  \\:\\ /  /:/  __|__|:|   /__/:/ /:/  \\__\\/\\:\\__  /__/:/ /:/ /\\ /__/:/ /:/___
                     \\  \\:\\/:/   \\  \\:\\ /  /:/  \\  \\:\\  /:/  /__/::::\\   \\  \\:\\/:/      \\  \\:\\/\\ \\  \\:\\/:/ /:/ \\  \\:\\/:::::/
                      \\  \\::/     \\  \\:\\  /:/    \\  \\:\\/:/      ~\\~~\\:\\   \\  \\::/        \\__\\::/  \\  \\::/ /:/   \\  \\::/~~~~\s
                       \\  \\:\\      \\  \\:\\/:/      \\  \\::/         \\  \\:\\   \\  \\:\\        /__/:/    \\  \\:\\/:/     \\  \\:\\    \s
                        \\  \\:\\      \\  \\::/        \\__\\/           \\__\\/    \\  \\:\\       \\__\\/      \\  \\::/       \\  \\:\\   \s
                         \\__\\/       \\__\\/                                   \\__\\/                   \\__\\/         \\__\\/   \s
                                                                                                                           \s
                                                    
                """;
        String segment = "\n" + "-".repeat(120) + "\n";
        log.info(segment + "\n" + banner + segment);
        log.info( "Polyfier-Server running... UI accessible on " + "http://" + serverConfig.getHost() + ":" + serverConfig.getPort() );
        log.info( "Polypheny-DB Backend connected on " + serverConfig.getUrl() + " and accessible on " + "http://" + serverConfig.getHost() + ":8080" );
        log.info( "If Polypheny-Control was used to start it, here is a link to that as well: " + "http://" + serverConfig.getHost() + ":8070" );
        log.info( segment );
    }


    public static void main(String[] args) {
        log.debug("CLI: " + Arrays.toString( args ) );

        if ( args.length == 0 ) {
            runServer();
        } else {
            try {
                CommandLine commandLine = clParser.parse( cliOptions, args );

                if ( commandLine.hasOption("r") ) {
                    String[] rArgs = commandLine.getOptionValues("r");
                    String host = rArgs[0];
                    int port = Integer.parseInt( rArgs[1] );
                    runServer( host, port );
                }

            } catch ( ParseException e ) {
                throw new RuntimeException( e );
            }
        }


    }

}
