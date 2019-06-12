package consumer;

import config.KafkaProperyies;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

public class UserConsumerSingle {
    private static Consumer<String,String> consumer;
    private static String cuTopic;
    private UserConsumerSingle(String topic){
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
    }
//    private static class UserConsumerSingelInstance{
//        private static UserConsumerSingle INSTANCE = new UserConsumerSingle(cuTopic);
//    }
    public static UserConsumerSingle getInstance(String topic){
        cuTopic = topic;
        if(consumer!=null){
            consumer.close();
        }
        UserConsumerSingle INSTANCE = new UserConsumerSingle(cuTopic);
        return INSTANCE;
    }

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
            consumerInfo.setKey(record.key());
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
            TopicPartition p = new TopicPartition(UserConsumerSingle.cuTopic,partition);
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
