package utils;

import com.sun.org.apache.xpath.internal.operations.Gte;
import config.HbaseConfig;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.List;

public class HbaseUtils implements IHbaseUtils{

    /**
     * 判断表是否存在
     * @param tableName
     * @return
     * @throws Exception
     */

    public Boolean isExistTable(String tableName) throws Exception{
        Connection connection = HbaseConfig.getConnection();
        Admin admin = connection.getAdmin();
        try {
            TableName name = TableName.valueOf(tableName);
            Boolean res = admin.tableExists(name);
            return res;
        }finally {
            admin.close();
        }
    }

    /**
     * 创建表
     * @param tableName
     * @param cols
     * @throws Exception
     */
    public void createTable(String tableName,String[] cols) throws Exception{
        Connection connection = HbaseConfig.getConnection();
        Admin admin = connection.getAdmin();
        TableName name = TableName.valueOf(tableName);
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(name);
        for(String col:cols){
            ColumnFamilyDescriptor colFamily = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(col)).build();
            tableDescriptorBuilder.setColumnFamily(colFamily);
        }
        TableDescriptor descriptor = tableDescriptorBuilder.build();
        admin.createTable(descriptor);
        admin.close();
    }

    /**
     * 删除表
     * @param tableName
     * @throws Exception
     */
    public void deleteTable(String tableName) throws Exception{
        Connection connection = HbaseConfig.getConnection();
        Admin admin = connection.getAdmin();
        try{
            TableName name = TableName.valueOf(tableName);
            admin.disableTable(name);
            admin.deleteTable(name);
        }finally {
            admin.close();
        }
    }

    /**
     * 执行表的数据的put update or delete
     * @param tableName
     * @param action
     */
    public void execute(String tableName, MutatorCallback action){
        Assert.assertNotNull("Callback object must not be null",action);
        Assert.assertNotNull("table must not be null",tableName);
        TableName name = TableName.valueOf(tableName);
        BufferedMutator mutator = null;
        BufferedMutatorParams mutatorParams = new BufferedMutatorParams(name);
        try {
            mutator = HbaseConfig.getConnection().getBufferedMutator(mutatorParams.writeBufferSize(1024*1024*5));
            action.doInMutator(mutator);
        }catch (Throwable e){
            e.printStackTrace();
        }finally {
            if(mutator!=null){
                try {
                    mutator.flush();
                    mutator.close();
                }catch (Exception e){
                    System.out.println("hbase mutator资源释放失败");
                }
            }
        }
    }

    /**
     *
     * @param tableName
     * @param mutation
     */
    public void saveOrUpdate(String tableName, final Mutation mutation) {
        this.execute(tableName, new MutatorCallback() {
            public void doInMutator(BufferedMutator mutator) throws Throwable {
                mutator.mutate(mutation);
            }
        });
    }
    /**
     * 批量执行
     * @param tableName
     * @param mutations
     */
    public void saveOrUpdates(String tableName, final List<Mutation> mutations) {
        this.execute(tableName, new MutatorCallback() {
            public void doInMutator(BufferedMutator mutator) throws Throwable {
                mutator.mutate(mutations);
            }
        });
    }
    /**
     *
     * @param tableName
     * @param action
     * @param <T>
     * @return
     */
    public <T> T execute(String tableName, TableCallback<T> action) {
        Assert.assertNotNull("Callback object must not be null",action);
        Assert.assertNotNull("table must not be null",tableName);
        TableName name = TableName.valueOf(tableName);
        Table table = null;
        try {
            table = HbaseConfig.getConnection().getTable(name);
            return action.doInTable(table);
        }catch (Throwable e){
            e.printStackTrace();

        }finally {
            try {
                table.close();
            }catch (Exception e){
                System.out.println("Hbase table 资源释放失败");
            }
        }
        return null;
    }

    public <T> List<T> find(String tableName, String familyName, RowMapper<T> mapper) {
        Scan scan = new Scan();
        scan.setCaching(5000);//设置每次请求的记录数
        scan.addFamily(Bytes.toBytes(familyName));
        return this.find(tableName,scan,mapper);
    }

    public <T> List<T> find(String tableName, String familyName, String qualifierName, RowMapper<T> mapper) {
        Scan scan = new Scan();
        scan.setCaching(5000);
        scan.addColumn(Bytes.toBytes(familyName),Bytes.toBytes(qualifierName));
        return this.find(tableName,scan,mapper);
    }

    public <T> List<T> find(String tableName, final Scan scan, final RowMapper<T> mapper) {
        return this.execute(tableName, new TableCallback<List<T>>() {
            public List<T> doInTable(Table table) throws Throwable {
                int caching = scan.getCaching();
                if(caching==1){
                    scan.setCaching(5000);
                }
                ResultScanner scanner = table.getScanner(scan);
                try {
                    List<T> res = new ArrayList<T>();
                    int rowNum = 0;
                    for(Result result:scanner){
                        res.add(mapper.mapRow(result,rowNum++));
                    }
                    return  res;
                }catch (Exception e){
                    e.printStackTrace();
                }finally {
                    scanner.close();
                }
                return null;
            }
        });
    }

    public <T> T get(String tableName, String rowKey, RowMapper<T> mapper) {
        return this.get(tableName,rowKey,null,null,mapper);
    }

    public <T> T get(String tableName, String rowKey, String familyName, RowMapper<T> mapper) {
        return this.get(tableName,rowKey,familyName,null,mapper);
    }

    public <T> T get(String tableName, final String rowKey, final String familyName, final String qualifierName, final RowMapper<T> mapper) {
        return this.execute(tableName, new TableCallback<T>() {
            public T doInTable(Table table) throws Throwable {
                Get get = new Get(Bytes.toBytes(rowKey));
                if(StringUtils.isNotBlank(familyName)){
                    byte[] family = Bytes.toBytes(familyName);
                    if(StringUtils.isNotBlank(qualifierName)){
                        get.addColumn(family,Bytes.toBytes(qualifierName));
                    }else{
                        get.addFamily(family);
                    }
                }
                Result result = table.get(get);
                return mapper.mapRow(result,0);
            }
        });
    }
}
