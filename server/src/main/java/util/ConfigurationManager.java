package util;

import java.util.Map;

public class ConfigurationManager {


    public ConfigurationManager() {

    }


    public static long hashOfString( String configurationName ) {
        long h = 1125899906842597L;
        int len = configurationName.length();
        for (int i = 0; i < len; i++) {
            h = 31*h + configurationName.charAt(i);
        }
        return h;
    }


    public static long hashOfBooleanConfig( Map<String, Boolean> configuration ) {
        long h = 0L;
        for ( String key : configuration.keySet() ) {
            if ( configuration.get( key ) ) {
                h += hashOfString( key );
            } else {
                h -= hashOfString( key );
            }
        }
        return h;
    }


}
