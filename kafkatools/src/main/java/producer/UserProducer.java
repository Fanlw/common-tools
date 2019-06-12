package producer;

import config.KafkaProperyies;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

public class UserProducer{
    private Producer<String,String> producer = null;
    private String topic;

    /**
     * 创建生产者
     * @param topic 使用主题
     */
    public UserProducer(String topic){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", KafkaProperyies.Broker_List);
        properties.put("acks",KafkaProperyies.Acks);
        properties.put("key.serializer",KafkaProperyies.Key_Serializer);
        properties.put("value.serializer",KafkaProperyies.Value_Serializer);
        properties.put("connections.max.idle.ms","-1");
        producer = new KafkaProducer<String, String>(properties);
        this.topic = topic;
    }

    /**
     * 生产消息
     * @param msg 消息内容
     */
    public void SendMsg(String msg){
        try {
            RecordMetadata record = producer.send(new ProducerRecord<String, String>(topic,"op", msg)).get();

        }catch (Exception ex){
            ex.printStackTrace();
        }

    }
    /**
     * 在不需要的时候关闭producer
     */
    public void Close(){
        producer.close();
    }
}
