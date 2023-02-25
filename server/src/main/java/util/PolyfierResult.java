package util;

import java.io.Serializable;


public class PolyfierResult implements Serializable {

    public String logical;
    public String physical;
    public String message;
    public String cause;
    public String result;
    public long seed;
    public long actual;
    public long predicted;
    public boolean success;

}
