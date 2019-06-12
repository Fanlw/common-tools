package utils;

import org.apache.hadoop.hbase.client.BufferedMutator;

public interface MutatorCallback {
    void doInMutator(BufferedMutator mutator) throws Throwable;
}
