package onegis.redistools;

import onegis.redistools.utils.PropertiesUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisInitialize {
    //IP
    private static String host = PropertiesUtils.getValue("host");
    //端口号
    private static int port = Integer.parseInt(PropertiesUtils.getValue("port"));
    //private static String auth = null;//访问密码
    private static String auth = PropertiesUtils.getValue("auth");//访问密码
    //可用连接实例的最大数目，默认值为8；
    //如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)
    private static int MAX_TOTAL=Integer.parseInt(PropertiesUtils.getValue("MAX_TOTAL")) ;
    //控制一个pool最多有多少个状态为idle(空闲的)的jedis实例，默认值也是8
    private static int MAX_IDLE = Integer.parseInt(PropertiesUtils.getValue("MAX_IDLE")) ;
    //等待可用连接的最大时间，单位毫秒，默认值为-1，表示永不超时。如果超过等待时间，则直接抛出JedisConnectionException；
    private static int MAX_WAIT = Integer.parseInt(PropertiesUtils.getValue("MAX_WAIT")) ;
    private static int TIMEOUT = Integer.parseInt(PropertiesUtils.getValue("TIMEOUT"));
    //在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
    private static boolean TEST_ON_BORROW = PropertiesUtils.getValue("TEST_ON_BORROW").equals("true");
    // 设置为0的话就是永远都不会过期
    private static int expire = 0;
    // 定义一个管理池，所有的redisManager共同使用。
    private static JedisPool jedisPool = null;
    //连接的数据库号
    private static int dbIndex = 0;
    public static void initialPool(int index){
        dbIndex =index;
        try{
            //连接池的基本配置
            JedisPoolConfig config = new JedisPoolConfig();
            config.setMaxTotal(MAX_TOTAL);
            config.setMaxIdle(MAX_IDLE);
            config.setMaxWaitMillis(MAX_WAIT);
            config.setTestOnBorrow(TEST_ON_BORROW);
            if(auth==""||auth==null||auth.equals("")){
                jedisPool = new JedisPool(config,host,port,TIMEOUT);
            }else{
                jedisPool = new JedisPool(config,host,port,TIMEOUT,auth);
            }

        }catch (Exception ex){
            ex.printStackTrace();
        }
    }
    public static synchronized Jedis getJedis(){
        try{
            if(jedisPool!=null){
                //指明Redis连接第几个库
                Jedis resource = jedisPool.getResource();
                resource.select(dbIndex);
                return  resource;
            }else {
                System.out.println("获取的Redis链接为null");
                return null;
            }
        }catch(Exception ex){
            ex.printStackTrace();
            return null;
        }
    }



}
