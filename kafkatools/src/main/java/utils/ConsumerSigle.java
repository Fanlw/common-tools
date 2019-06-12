package utils;


import consumer.UserConsumer;

public class ConsumerSigle {
    private static String tp;
    private ConsumerSigle(String topic){

    }
    private static class ConsumerSingleInstance{
        private static final ConsumerSigle INSTANCE = new ConsumerSigle(tp);
    }

    public static ConsumerSigle getInstance(String topic){
        tp = topic;
        return ConsumerSingleInstance.INSTANCE;
    }
}
