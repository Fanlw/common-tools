package consumer;

import config.KafkaProperyies;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;


import java.time.Duration;
import java.util.*;

/**
 * 消费者
 * @author flw
 */
public class UserConsumer {

    private Consumer<String,String> consumer;
    private String topic;
    /**
     * 创建消费者
     * @param topic 主题
     */
    public  UserConsumer(String topic){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", KafkaProperyies.Broker_List);
        properties.put("group.id","Ges");
        properties.put("enable.auto.commit",KafkaProperyies.Enable_Auto_Commit);
        properties.put("auto.commit.interval.ms",KafkaProperyies.Auto_Commit_Interval_ms);
        properties.put("session.timeout.ms",KafkaProperyies.Session_Timeout_ms);
        properties.put("key.deserializer",KafkaProperyies.Key_Deserializer);
        properties.put("value.deserializer",KafkaProperyies.Value_Deserializer);
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topic));
        this.topic = topic;
    }

    /**
     * 拉取消息
     * @param millis 拉取间隔时间 ms
     * @return 拉取到的信息
     */
    public List<ConsumerInfo> poll(long millis){
        List<ConsumerInfo> res = new ArrayList<>();
        //List<String> result = new ArrayList<>();
//        TopicPartition p = new TopicPartition(topic,0);
//        consumer.assign(Arrays.asList(p));
//        consumer.seek(p,0);
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(millis));
        for(ConsumerRecord<String,String> record:records){
            //result.add(record.value());
            ConsumerInfo consumerInfo = new ConsumerInfo();
            consumerInfo.setValue(record.value());
            consumerInfo.setOffset(record.offset());
            consumerInfo.setPartition(record.partition());
            res.add(consumerInfo);
//            if(record.offset()==17){
//                TopicPartition p = new TopicPartition(topic,record.partition());
//                OffsetAndMetadata m = new OffsetAndMetadata(record.offset());
//                Map<TopicPartition,OffsetAndMetadata> map = new HashMap<>();
//                map.put(p,m);
//                consumer.commitSync(map);
//            }
            System.out.println("当前偏移量："+record.offset());
        }

        return res;
    }

    public Boolean commitSync(int partition,long offset){
        try{
            TopicPartition p = new TopicPartition(topic,partition);
            OffsetAndMetadata m = new OffsetAndMetadata(offset);
            Map<TopicPartition,OffsetAndMetadata> map = new HashMap<>();
            map.put(p,m);
            consumer.commitSync(map);
            return true;
        }catch (Exception ex){
            ex.printStackTrace();
            return false;
        }

    }

    /**
     * 不使用时需关闭
     */
    public void close(){
        consumer.close();
    }

}
