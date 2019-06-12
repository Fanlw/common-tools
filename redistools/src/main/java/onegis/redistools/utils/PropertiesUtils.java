package onegis.redistools.utils;

import java.util.ResourceBundle;

public class PropertiesUtils {
    public static String getValue(String key){
        ResourceBundle config = ResourceBundle.getBundle("config");
        String  o = null;
        try {
            o = config.getString(key);
            return o;
        }catch (Exception ex){
            return o;
        }
    }
}
