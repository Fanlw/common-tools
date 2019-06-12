import producer.UserProducer;

public class KafkaInit {
    public static UserProducer producer ;

    public KafkaInit(String topic){
        producer = new UserProducer(topic);
    }

}
