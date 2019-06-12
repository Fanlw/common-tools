package onegis.redistools.utils;

import onegis.redistools.RedisInitialize;
import redis.clients.jedis.Jedis;

public class RedisByteUtils {

    public static byte[] get(byte[] key){
        byte[] value = null;
        Jedis jedis = RedisInitialize.getJedis();
        try {
            value = jedis.get(key);
        }finally {
            jedis.close();
        }
        return value;
    }

    public static String set(byte[] key, byte[] value) {
        Jedis jedis = RedisInitialize.getJedis();
        String set ="";
        try {
            set = jedis.set(key, value);
        } finally {
            jedis.close();
            return set;
        }
    }

    public static String set(byte[] key, byte[] value,int expire) {
        Jedis jedis = RedisInitialize.getJedis();
        String result ="";
        try {
            result=jedis.set(key, value);
            if (expire != 0) {
                jedis.expire(key, expire);
            }
        } finally {
            jedis.close();
            return result;
        }
    }

    public static Long del(byte[] key){
        Jedis jedis = RedisInitialize.getJedis();
        Long delNum =0l;
        try {
            delNum = jedis.del(key);
        } finally {
            jedis.close();
            return delNum;
        }
    }

    public static boolean exists(byte[] key){
        Jedis jedis = RedisInitialize.getJedis();
        boolean sign = jedis.exists(key);
        jedis.close();
        return  sign;
    }

    public static Long ttlKey(byte[] key){
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
