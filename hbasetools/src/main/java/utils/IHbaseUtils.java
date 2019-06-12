package utils;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Scan;

import java.util.List;

public interface IHbaseUtils {
    /**
     * 根据表名判断表是否存在
     * @param tableName
     * @return
     * @throws Exception
     */
    Boolean isExistTable(String tableName) throws Exception;
    void createTable(String tableName,String[] cols) throws Exception;
    void deleteTable(String tableName) throws Exception;
    void execute(String tableName,MutatorCallback action);
    void saveOrUpdate(String tableName, Mutation mutation);
    void saveOrUpdates(String tableName, List<Mutation> mutations);
    <T> T execute(String tableName,TableCallback<T> action);
    <T> List<T> find(String tableName,String familyName,final RowMapper<T> mapper);
    <T> List<T> find(String tableName,String familyName,String qualifierName,final RowMapper<T> mapper);
    <T> List<T> find(String tableName, Scan scan,final RowMapper<T> mapper);
    <T> T get(String tableName,String rowKey,final RowMapper<T> mapper);
    <T> T get(String tableName,String rowKey,String familyName,final RowMapper<T> mapper);
    <T> T get(String tableName,String rowKey,String familyName,String qualifierName,final RowMapper<T> mapper);


}
