package utils;

import config.KafkaProperyies;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;

import java.util.Arrays;
import java.util.Properties;
import java.util.Set;


/**
 * Topic操作
 * @author flw
 */
public class KafkaTopicUtils {

    /**
     * 创建主题
     * @param topic  主题名称
     * @param numPartition  分区数
     * @param replicationFactor 副本因子
     */
    public static void createTopic(String topic,int numPartition,short replicationFactor){
        AdminClient adminClient;
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,KafkaProperyies.Broker_List);
        adminClient = AdminClient.create(properties);
        NewTopic newTopic = new NewTopic(topic,numPartition,replicationFactor);
        CreateTopicsResult topics = adminClient.createTopics(Arrays.asList(newTopic));
        adminClient.close();
    }

    /**
     * 删除主题
     * @param topic 主题名
     */
    public static void deleteTopic(String topic){
        AdminClient adminClient;
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, KafkaProperyies.Broker_List);
        adminClient = AdminClient.create(properties);
        adminClient.deleteTopics(Arrays.asList(topic));
        adminClient.close();
    }

    /**
     * 罗列所有的主题名
     * @return 主题名列表
     * @throws Exception
     */
    public static Set<String> listTopic() throws Exception{
        AdminClient adminClient;
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, KafkaProperyies.Broker_List);
        adminClient = AdminClient.create(properties);
        ListTopicsResult listTopicsResult = adminClient.listTopics();
        KafkaFuture<Set<String>> names = listTopicsResult.names();
        return names.get();
    }
}
