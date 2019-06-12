package onegis.redistools.utils;

import onegis.redistools.RedisInitialize;
import redis.clients.jedis.Jedis;

public class RedisStringUtils {
    /**
     * 获取键值对应的内容
     * @param key 键值
     * @return 键值对应的内容
     */
    public static String get(String key){
        String value = null;
        Jedis jedis = RedisInitialize.getJedis();
        try {
            value = jedis.get(key);
        }finally {
            jedis.close();
        }
        return value;
    }
    /**
     * 设置key，vl
     * @param key 键值
     * @param value 内容
     * @return 成功返回OK
     */
    public static String set(String key,String value){
        Jedis jedis = RedisInitialize.getJedis();
        String result ="";
        try{
            result = jedis.set(key,value);
        }finally {
            jedis.close();
            return  result;
        }
    }
    /**
     * 设置key，vl及key的生存时间
     * @param key 键值
     * @param value 内容
     * @param expire 生成时间，单位是毫秒
     * @return 成功返回OK
     */
    public static String set(String key,String value,int expire){
        Jedis jedis = RedisInitialize.getJedis();
        String set ="";
        try{
            set = jedis.set(key, value);
            if(expire!=0){
                jedis.expire(key,expire);
            }
        }finally {
            jedis.close();
            return set;
        }
    }
    /**
     * 根据键值删除
     * @param key  键值
     * @return 成功执行个数
     */
    public static Long del(String key){
        Jedis jedis = RedisInitialize.getJedis();
        Long result = 0l;
        try {
            result = jedis.del(key);
        } finally {
            jedis.close();
            return result;
        }
    }
    /**
     * 判断键值是否存在
     * @param key 键值
     * @return
     */
    public static boolean exists(String key){
        Jedis jedis = RedisInitialize.getJedis();
        boolean sign = jedis.exists(key);
        jedis.close();
        return  sign;
    }
    /**
     * 获取键值对应的生死时间
     * @param key 查询键值
     * @return  当 key 不存在时，返回 -2 。
     * 当 key 存在但没有设置剩余生存时间时，返回 -1 。
     * 否则，以毫秒为单位，返回 key 的剩余生存时间。
     * 如果返回0，说明发生了错误
     */
    public static Long ttlKey(String key){
        Jedis jedis = RedisInitialize.getJedis();
        try{
            return jedis.ttl(key);
        }catch (Exception ex){
            ex.printStackTrace();
            return 0l;
        }finally {
            jedis.close();
        }
    }

}
