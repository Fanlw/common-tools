import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import utils.HbaseUtils;
import utils.IHbaseUtils;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;

public class TestUtils {
    public static void main(String[] args) {
        //createTable();
        //isExistTable();
        //saveOrUpdates();
        //get();
        //query();
        savaFile();
        System.out.println("执行完成");
    }

    //存储图片文件，单元格默认最大为10M
    public static void savaFile(){
        try {
            HbaseUtils hbaseUtils = new HbaseUtils();
            String imgPath = "D:\\Desktop\\111.jpg";
            FileInputStream fileInputStream = new FileInputStream(imgPath);
            byte[] file = new byte[fileInputStream.available()];
            fileInputStream.read(file);
            fileInputStream.close();
            Put put = new Put(Bytes.toBytes("1"));
            put.addColumn(Bytes.toBytes("msg1"),Bytes.toBytes("name"),Bytes.toBytes("111.jpg"));
            put.addColumn(Bytes.toBytes("msg2"),Bytes.toBytes("info"),file);
            hbaseUtils.saveOrUpdate("test",put);
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    public static void createTable(){
        try {
            HbaseUtils hbaseUtils = new HbaseUtils();
            hbaseUtils.createTable("test",new String[]{"msg1","msg2"});
        }catch (Exception e){
            e.printStackTrace();
        }

    }
    public static void isExistTable(){
        try{
            HbaseUtils hbaseUtils = new HbaseUtils();
            Boolean test = hbaseUtils.isExistTable("test");
            System.out.println("test 存在："+test);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public static void deleteTable(){
        try{
            HbaseUtils hbaseUtils = new HbaseUtils();
            hbaseUtils.deleteTable("test");
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void saveOrUpdates(){
        try{
            HbaseUtils hbaseUtils = new HbaseUtils();
            List<Mutation> saveOrUpdates = new ArrayList<Mutation>();
            Put put = new Put(Bytes.toBytes("1"));
            put.addColumn(Bytes.toBytes("msg1"),Bytes.toBytes("name"),Bytes.toBytes("tom"));
            put.addColumn(Bytes.toBytes("msg2"),Bytes.toBytes("info"),Bytes.toBytes("Am"));
            saveOrUpdates.add(put);
            Put put2 = new Put(Bytes.toBytes("2"));
            put2.addColumn(Bytes.toBytes("msg1"),Bytes.toBytes("name"),Bytes.toBytes("jim"));
            put2.addColumn(Bytes.toBytes("msg2"),Bytes.toBytes("info"),Bytes.toBytes("jpa"));
            saveOrUpdates.add(put2);
            Delete delete = new Delete(Bytes.toBytes("2"));
            saveOrUpdates.add(delete);
            hbaseUtils.saveOrUpdates("test",saveOrUpdates);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void get(){
        try {
            HbaseUtils hbaseUtils = new HbaseUtils();
            User user = hbaseUtils.get("test", "1", new UserRowMapper());
            System.out.println("name:"+user.getName()+"\r\ninfo:"+user.getInfo());
        }catch (Throwable e){
            e.printStackTrace();
        }
    }

    public static void query(){
        try{
            HbaseUtils hbaseUtils = new HbaseUtils();
            String start ="1";
            String end = "2";
            //Scan scan = new Scan(Bytes.toBytes(start),Bytes.toBytes(end));
            Scan scan = new Scan();
            scan.withStartRow(Bytes.toBytes(start));
            scan.withStopRow(Bytes.toBytes(end));
            scan.setCaching(10);
            List<User> test = hbaseUtils.find("test", scan, new UserRowMapper());
            System.out.println("len:"+test.size());
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
