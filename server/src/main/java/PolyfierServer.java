import connect.QueryLogConnection;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import server.Server;
import server.ServerConfig;

import java.util.Arrays;

@Slf4j
public class PolyfierServer {

    private static final Logger LOGGER;
    private static final CommandLineParser clParser;
    private static final HelpFormatter hFormatter;
    private static final Options cliOptions;


    static {
        // Logging  ---------------------------------------------------
        LOGGER = LogManager.getRootLogger();
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
        hFormatter = new HelpFormatter();
    }

    private static void runServer( String host, int port ) {
        ServerConfig serverConfig = ServerConfig.fetch();
        serverConfig.setAddress( host, port );
        runServer();
    }

    private static void runServer() {
        LOGGER.info("Loading Configurations...");
        ServerConfig serverConfig = ServerConfig.fetch();
        if ( ! serverConfig.hasAddress() ) {
            serverConfig.setAddress( "localhost", 44567 );
        }
        if ( ! serverConfig.hasUrl() ) {
            serverConfig.setUrl( "jdbc:polypheny://localhost:20591/" );
        }
        if ( ! serverConfig.hasCredentials() ) {
            serverConfig.setCredentials("pa", "");
        }
        LOGGER.info("Connecting to PolyphenyDB Backend...");
        QueryLogConnection queryLogConnection = connectPolyphenyDB( serverConfig );
        LOGGER.info("Connection established.");
        runServer( serverConfig, queryLogConnection );
    }

    private static QueryLogConnection connectPolyphenyDB( ServerConfig serverConfig ) {
        try {
            return new QueryLogConnection( serverConfig.getUrl(), serverConfig.getUser(), serverConfig.getPassword() );
        } catch ( Exception e ) {
            throw new RuntimeException("Could not establish connection to PolyphenyDB, is PolyphenyDB running and the port correct?", e );
        }
    }

    private static void runServer(ServerConfig serverConfig, QueryLogConnection queryLogConnection ) {
        LOGGER.info("Configuring Server...");
        Server polyfierServer = new Server( serverConfig, queryLogConnection );
        LOGGER.info("Server Configured.");
        LOGGER.info("Running Server...");
        try {
            polyfierServer.run();
        } catch ( Exception e ) {
            throw new RuntimeException( e );
        }
        displayBanner( serverConfig );
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
        LOGGER.info(new StringBuilder().append( segment ).append("\n").append( banner ).append( segment ).toString() );
        LOGGER.info( "Polyfier-Server running... UI accessible on " + "http://" + serverConfig.getHost() + ":" + serverConfig.getPort() );
        LOGGER.info( "Polypheny-DB Backend connected on " + serverConfig.getUrl() + " and accessible on " + "http://" + serverConfig.getHost() + ":8080" );
        LOGGER.info( "If Polypheny-Control was used to start it, here is a link to that as well: " + "http://" + serverConfig.getHost() + ":8070" );
        LOGGER.info( segment );
    }


    public static void main(String[] args) {
        LOGGER.debug("CLI: " + Arrays.toString( args ) );

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


        // Todo manage backend with this thread.
        try {
            while ( true ) {
                Thread.sleep( 10000 );
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }


    }

}
