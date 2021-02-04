package simpledb.iterator;

import simpledb.dbrecord.Record;
import simpledb.matadata.table.TableDesc;

/**
 * @author xiongyx
 * @date 2021/2/4
 */
public interface DbFileIterator<T> {

    void open();

    void close();

    void reset();

    boolean hasNext();

    Record next();
}
