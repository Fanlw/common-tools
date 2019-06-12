package onegis.redistools.utils;

import onegis.redistools.RedisInitialize;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RedisPipelineUtils {
    public static Map<String,String> setString(Map<String,String> info){
        Jedis jedis = RedisInitialize.getJedis();
        Map<String,String> result = new HashMap<String, String>();
        try{
            Pipeline pipelined = jedis.pipelined();
            Map<String,Response<String>> responseMap = new HashMap<String, Response<String>>();
            for(String key:info.keySet()){
                Response<String> set = pipelined.set(key, info.get(key));
                responseMap.put(key,set);
            }
            pipelined.sync();
            for(String key:info.keySet()){
                result.put(key,responseMap.get(key).get());
            }
        }finally {
            jedis.close();
            return result;
        }
    }

    public static Map<String,String> setString(Map<String,String> info,int expire){
        Jedis jedis = RedisInitialize.getJedis();
        Map<String,String> result = new HashMap<String, String>();
        try{
            Pipeline pipelined = jedis.pipelined();
            Map<String,Response<String>> responseMap = new HashMap<String, Response<String>>();
            for(String key:info.keySet()){
                Response<String> set = pipelined.set(key, info.get(key));
                pipelined.expire(key,expire);
                responseMap.put(key,set);
            }
            pipelined.sync();
            for(String key:info.keySet()){
                result.put(key,responseMap.get(key).get());
            }
        }finally {
            jedis.close();
            return result;
        }
    }

    public static Map<String,String> getString(List<String> keyList){
        Jedis jedis = RedisInitialize.getJedis();
        Map<String,String> result = new HashMap<String, String>();
        try{
            Pipeline pipelined = jedis.pipelined();
            Map<String,Response<String>> responseMap = new HashMap<String, Response<String>>();
            for(String key:keyList){
                Response<String> stringResponse = pipelined.get(key);
                responseMap.put(key,stringResponse);
            }
            pipelined.sync();
            for(String key:keyList){
                result.put(key,responseMap.get(key).get());
            }
        }catch (Exception ex){
            ex.printStackTrace();
        }finally{
            jedis.close();
            return  result;
        }
    }

    public static Map<byte[],String> setByte(Map<byte[],byte[]> info){
        Jedis jedis = RedisInitialize.getJedis();
        Map<byte[],String> result = new HashMap<byte[], String>();
        try{
            Pipeline pipelined = jedis.pipelined();
            Map<byte[],Response<String>> responseMap = new HashMap<byte[], Response<String>>();
            for(byte[] key:info.keySet()){
                Response<String> set = pipelined.set(key, info.get(key));
                responseMap.put(key,set);
            }
            pipelined.sync();
            for(byte[] key:info.keySet()){
                result.put(key,responseMap.get(key).get());
            }
        }finally {
            jedis.close();
            return  result;
        }
    }

    public static Map<byte[],String> setByte(Map<byte[],byte[]> info,int expire){
        Jedis jedis = RedisInitialize.getJedis();
        Map<byte[],String> result = new HashMap<byte[], String>();
        try{
            Pipeline pipelined = jedis.pipelined();
            Map<byte[],Response<String>> responseMap = new HashMap<byte[], Response<String>>();
            for(byte[] key:info.keySet()){
                Response<String> set = pipelined.set(key, info.get(key));
                pipelined.expire(key,expire);
                responseMap.put(key,set);
            }
            pipelined.sync();
            for(byte[] key:info.keySet()){
                result.put(key,responseMap.get(key).get());
            }
        }finally {
            jedis.close();
            return result;
        }
    }

    public static Map<byte[],byte[]> getByte(List<byte[]> keyList){
        Jedis jedis = RedisInitialize.getJedis();
        Map<byte[],byte[]> result = new HashMap<byte[], byte[]>();
        try{
            Pipeline pipelined = jedis.pipelined();
            Map<byte[],Response<byte[]>> responseMap = new HashMap<byte[], Response<byte[]>>();
            for(byte[] key:keyList){
                Response<byte[]> stringResponse = pipelined.get(key);
                responseMap.put(key,stringResponse);
            }
            pipelined.sync();
            for(byte[] key:keyList){
                result.put(key,responseMap.get(key).get());
            }
        }catch (Exception ex){
            ex.printStackTrace();
        }finally{
            jedis.close();
            return result;
        }
    }

    public static Map<String,Long> delString(List<String> keyList){
        Jedis jedis = RedisInitialize.getJedis();
        Map<String,Long> result = new HashMap<String, Long>();
        try{
            Pipeline pipelined = jedis.pipelined();
            Map<String,Response<Long>> responseMap = new HashMap<String, Response<Long>>();
            for(String key:keyList){
                Response<Long> del = pipelined.del(key);
                responseMap.put(key,del);
            }
            pipelined.sync();
            for(String key:keyList){
                result.put(key,responseMap.get(key).get());
            }
        }finally {
            jedis.close();
            return  result;
        }
    }

    public static Map<byte[],Long> delByte(List<byte[]> keyList){
        Jedis jedis = RedisInitialize.getJedis();
        Map<byte[],Long> result = new HashMap<byte[], Long>();
        try{
            Pipeline pipelined = jedis.pipelined();
            Map<byte[],Response<Long>> responseMap = new HashMap<byte[], Response<Long>>();
            for(byte[] key:keyList){
                Response<Long> del = pipelined.del(key);
                responseMap.put(key,del);
            }
            pipelined.sync();
            for(byte[] key:keyList){
                result.put(key,responseMap.get(key).get());
            }
        }finally {
            jedis.close();
            return  result;
        }
    }
}
