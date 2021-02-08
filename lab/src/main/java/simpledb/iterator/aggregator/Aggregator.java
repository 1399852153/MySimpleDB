package simpledb.iterator.aggregator;

import simpledb.dbrecord.Record;
import simpledb.iterator.operator.DbIterator;

/**
 * @author xiongyx
 * @date 2021/2/7
 */
public interface Aggregator {
    static final int NO_GROUPING = -1;


    void mergeNewRecord(Record record);

    DbIterator iterator();
}
