package config;

import utils.PropertiesUtils;

/**
 * 配置信息
 * @author flw
 */
public class KafkaProperyies {

    /**
     * zookeeper地址
     */
    public static final String ZkConnect = PropertiesUtils.getValue("zkConnect");
    /**
     * broker地址
     */
    public static final String Broker_List = PropertiesUtils.getValue("broker.list");
    /**
     * 生产者发送消息后的应发方式   all,0,1,-1   其中all和-1等价
     */
    public static final String Acks = PropertiesUtils.getValue("acks");
    /**
     * key序列化方法
     */
    public static final String Key_Serializer =PropertiesUtils.getValue("key.serializer");
    /**
     * value序列化方法
     */
    public static final String Value_Serializer=PropertiesUtils.getValue("value.serializer");
    /**
     * key反序列化方法
     */
    public static final String Key_Deserializer= PropertiesUtils.getValue("key.deserializer");
    /**
     * value反序列化方法
     */
    public static final String Value_Deserializer=PropertiesUtils.getValue("value.deserializer");
    /**
     * 客户端自动提交偏移量
     */
    public static final String Enable_Auto_Commit = PropertiesUtils.getValue("enable.auto.commit");
    /**
     * 自动提交的间隔时间
     */
    public static final String Auto_Commit_Interval_ms = PropertiesUtils.getValue("auto.commit.interval.ms");
    /**
     * 连接超时时间
     */
    public  static final String Session_Timeout_ms = PropertiesUtils.getValue("session.timeout.ms");

}
