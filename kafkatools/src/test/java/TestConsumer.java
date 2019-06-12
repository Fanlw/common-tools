import com.fasterxml.jackson.databind.ObjectMapper;
import consumer.ConsumerInfo;
import consumer.UserConsumer;
import consumer.UserConsumerSingle;
import jdk.internal.dynalink.beans.StaticClass;
import scala.reflect.internal.Trees;
import utils.ConsumerSigle;
import utils.Kafka;

import java.sql.SQLOutput;
import java.util.List;

public class TestConsumer {
    public static void main(String[] args) {
        UserConsumerSingle single = UserConsumerSingle.getInstance("es");
        while(true){
            List<ConsumerInfo> poll = single.poll(1000);
            for(ConsumerInfo info:poll){
                System.out.println("key:"+info.getKey());
                System.out.println("value:"+info.getValue());
            }
            System.out.println("poll:"+poll.size());
        }

     //   single.close();
//        System.out.println("此处讲重新获得实例");
//        UserConsumerSingle single1 = UserConsumerSingle.getInstance("es");
//        List<ConsumerInfo> poll1 = single1.poll(1000);
//        System.out.println("NewPoll:"+poll1.size());
//        single1.close();
//        Kafka.setInit("es");
//        List<ConsumerInfo> poll = Kafka.poll(1000);
//        for(ConsumerInfo info:poll){
//            System.out.println("partition:"+info.getPartition()+"---offset:"+info.getOffset());
//            if(info.getOffset()==18){
//                Kafka.commitSync(info.getPartition(),info.getOffset());
//            }
//        }
//        System.out.println("拉取信息："+poll.size());
//        Kafka.close();
//        Kafka.setInit("es");
//        List<ConsumerInfo> poll1 = Kafka.poll(1000);
//        for(ConsumerInfo info:poll1){
//            System.out.println("partition:"+info.getPartition()+"---offset:"+info.getOffset());
//        }
//        System.out.println("new拉取信息："+poll1.size());
//        Kafka.close();




//        ObjectMapper mapper = new ObjectMapper();
//        UserConsumer userConsumer = new UserConsumer("es");
//
//        List<String> poll = userConsumer.poll(1000);
//        System.out.println("拉取信息："+poll.size());


//        while(true){
//            List<String> poll = userConsumer.poll(1000);
//            System.out.println("拉取信息");
//            for(String s:poll){
//                try {
//                    System.out.println("数据信息："+s);
//                }catch (Exception ex){
//                    ex.printStackTrace();
//                }
//
//            }
//        }
    }
}
