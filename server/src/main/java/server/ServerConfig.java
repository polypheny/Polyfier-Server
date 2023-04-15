package server;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.SystemUtils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

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

    public void setUrl(String url ) {
        this.url = url;
        this.save();
    }

    public void setCredentials(String user, String password ) {
        this.user = user;
        this.password = password;
        this.save();
    }

    public void setAddress(String host, int port ) {
        this.host = host;
        this.port = port;
        this.save();
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

    private void save() {
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
    }

}
