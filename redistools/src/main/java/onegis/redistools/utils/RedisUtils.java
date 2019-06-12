package onegis.redistools.utils;

import onegis.redistools.RedisInitialize;
import redis.clients.jedis.Jedis;

public class RedisUtils {
    /**
     * 清空当前数据库
     * @return 成功返回 OK
     */
    public static String flushDb(){
        Jedis jedis = RedisInitialize.getJedis();
        String res ="";
        try {
            res = jedis.flushDB();
        }finally {
            jedis.close();
            return res;
        }
    }

    /**
     * 清空所有库的数据
     * @return 成功返回 Ok
     */
    public static String flushAll(){
        Jedis jedis = RedisInitialize.getJedis();
        String res ="";
        try {
            res = jedis.flushAll();
        }finally {
            jedis.close();
            return res;
        }
    }

    /**
     * 获取当前库中的key数
     * @return 键的个数
     */
    public static Long getKeysNum(){
        Jedis jedis = RedisInitialize.getJedis();
        Long res = 0l;
        try {
            res = jedis.dbSize();
        }finally {
            jedis.close();
            return res;
        }
    }
}
