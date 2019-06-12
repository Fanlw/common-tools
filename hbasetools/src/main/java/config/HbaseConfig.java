package config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;


public class HbaseConfig {
    private static Connection conn;
    private static void initHbase() throws Exception{
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum","192.168.1.184:2181,192.168.1.185:2181,192.168.1.186:2181");
        config.set("hbase.zookeeper.property.clientPort","2181");
        config.set("hbase.master", "192.168.1.184:60010");
        conn = ConnectionFactory.createConnection(config);
    }

    public static synchronized Connection getConnection() throws Exception{
        if(conn==null||conn.isClosed()){
            initHbase();
        }
        return  conn;
    }
}
