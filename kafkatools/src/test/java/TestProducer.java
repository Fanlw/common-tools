import producer.UserProducer;

public class TestProducer {
    public static void main(String[] args) {
        UserProducer userProducer = new UserProducer("es");
        int index =0;
        userProducer.SendMsg("hahahahah");
        userProducer.SendMsg("ddddddd");
        userProducer.Close();
        System.out.println("生产者执行结束");
    }
}
