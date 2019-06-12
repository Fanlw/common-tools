package utils;

import java.util.ResourceBundle;

/**
 * 读取配置文件
 * @author flw
 */
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
