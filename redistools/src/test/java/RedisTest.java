import onegis.redistools.RedisInitialize;
import onegis.redistools.utils.RedisPipelineUtils;
import onegis.redistools.utils.RedisStringUtils;
import onegis.redistools.utils.RedisUtils;

import java.io.File;
import java.io.InputStream;
import java.util.*;

public class RedisTest {
    public static void main(String[] args) {
        RedisInitialize.initialPool(1);//初始化
        Long keysNum = RedisUtils.getKeysNum();
        System.out.println(keysNum);
//        RedisStringUtils.set("name","blue");
//        String name = RedisStringUtils.get("name");
//        System.out.println(name);
//        Map<String,String> mapInfo = new HashMap<String, String>();
//        List<String> ls = new ArrayList<String>();
//        for(int i=0;i<100;i++){
//            mapInfo.put(i+"",i+"");
//            ls.add(i+"");
//        }
//        RedisPipelineUtils.setString(mapInfo);
//        Map<String, String> stringM = RedisPipelineUtils.getString(ls);
//        for(String s:stringM.keySet()){
//            System.out.println(s);
//        }
//
//        Map<String, String> stringMMM = RedisPipelineUtils.getString(ls);
//        for(String s:stringMMM.keySet()){
//            System.out.println("de:"+s);
//        }
    }
}
