package server;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.SystemUtils;
import server.generators.PolyphenyDbProfileGenerator;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ServerConfig implements Serializable {
    private static final String APP_DIR = ".polyfier";
    private static final String CONFIG_FILE = "config.json";

    // Configurations ---- (~/.polyfier)
    @Getter
    private String url;
    @Getter
    private String user;
    @Getter
    private String password;
    @Getter
    private String host;
    @Getter
    private Integer port;

    // --------------------------------

    public boolean hasAddress() {
        return this.host != null && this.port != null;
    }

    public boolean hasUrl() {
        return this.url != null;
    }

    public boolean hasCredentials() {
        return this.user != null && this.password != null;
    }

    public ServerConfig setUrl( String url ) {
        this.url = url;
        return this.save();
    }

    public ServerConfig setCredentials( String user, String password ) {
        // Todo Encrypt / Decrypt...
        this.user = user;
        this.password = password;
        return this.save();
    }

    public ServerConfig setAddress( String host, int port ) {
        this.host = host;
        this.port = port;
        return this.save();
    }

    public static ServerConfig fetch() {
        return new ServerConfig().refresh();
    }

    private ServerConfig refresh() {
        if ( exists() ) {
            return get();
        }
        return this;
    }

    private static Path getAppDirPath() {
        return Paths.get(SystemUtils.getUserHome().getAbsolutePath(), APP_DIR);
    }

    private static Path getConfigPath() {
        return Paths.get(SystemUtils.getUserHome().getAbsolutePath(), APP_DIR, CONFIG_FILE);
    }

    private static boolean exists() {
        return getConfigPath().toFile().exists();
    }

    private static ServerConfig get() {
        Gson gson = new Gson();
        try {
            return gson.fromJson(new JsonReader(new FileReader(getConfigPath().toFile())), ServerConfig.class);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private ServerConfig save() {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        try {
            if ( ! Files.exists(getConfigPath()) ) {
                Files.createDirectory(getAppDirPath());
                Files.createFile(getConfigPath());
            }
            JsonWriter jsonWriter = new JsonWriter(new FileWriter(getConfigPath().toFile()));
            gson.toJson(this, ServerConfig.class, jsonWriter);
            jsonWriter.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    public static PolyphenyDbProfileGenerator getDefaultGenerator() {
        HashMap<String, String> dataConfigPreset = new HashMap<>();

        dataConfigPreset.put( "scale", "default");
        dataConfigPreset.put("ConfigKey1", "ConfigValue1");
        dataConfigPreset.put("ConfigKey2", "ConfigValue2");
        dataConfigPreset.put("ConfigKey3", "ConfigValue3");
        dataConfigPreset.put("ConfigKey4", "ConfigValue4");

        HashMap<String, String> queryConfigPreset = new HashMap<>();

        queryConfigPreset.put( "complexity", "4");
        queryConfigPreset.put("ConfigKey1", "ConfigValue1");
        queryConfigPreset.put("ConfigKey2", "ConfigValue2");
        queryConfigPreset.put("ConfigKey3", "ConfigValue3");
        queryConfigPreset.put("ConfigKey4", "ConfigValue4");


        HashMap<String, String> storeConfigPreset = new HashMap<>();
        List<String> storeConfigPermeable = List.of("HSQLDB", "POSTGRESQL", "MONETDB", "MONGODB" );
        storeConfigPreset.put( "COTTONTAIL", "false");
        storeConfigPreset.put( "NEO4J", "false");
        storeConfigPreset.put( "CASSANDRA", "false");


        HashMap<String, String> partConfigPreset = new HashMap<>();
        partConfigPreset.put("ConfigKey1", "ConfigValue1");
        partConfigPreset.put("ConfigKey2", "ConfigValue2");
        partConfigPreset.put("ConfigKey3", "ConfigValue3");
        partConfigPreset.put("ConfigKey4", "ConfigValue4");


        HashMap<String, String> startConfigPreset = new HashMap<>();
        startConfigPreset.put("ConfigKey1", "ConfigValue1");
        startConfigPreset.put("ConfigKey2", "ConfigValue2");
        startConfigPreset.put("ConfigKey3", "ConfigValue3");
        startConfigPreset.put("ConfigKey4", "ConfigValue4");



        return PolyphenyDbProfileGenerator.create(
                null,
                dataConfigPreset,
                null,
                queryConfigPreset,
                storeConfigPermeable,
                storeConfigPreset,
                null,
                partConfigPreset,
                null,
                startConfigPreset,
                false
        );

    }

}
