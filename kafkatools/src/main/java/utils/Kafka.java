package utils;

import consumer.ConsumerInfo;
import consumer.UserConsumer;

import java.util.List;

public class Kafka {
    private static boolean init = false;
    private static UserConsumer consumer;
    public static void setInit(String topic){
        consumer = new UserConsumer(topic);
        init = true;
    }
    public static List<ConsumerInfo> poll(long millis){
        if(init){
            return consumer.poll(millis);
        }else {
            return null;
        }
    }

    public static Boolean commitSync(int partition,long offset){
        return consumer.commitSync(partition,offset);
    }
    public static void close(){
        if(init){
            consumer.close();
            init = false;
        }else{
            System.out.println("WARN:消费者未初始化");
        }
    }
}
