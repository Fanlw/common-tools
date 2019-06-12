package utils;

import org.apache.hadoop.hbase.client.Result;

public interface RowMapper<T> {
    T mapRow(Result result,int rowNum) throws Throwable;
}
