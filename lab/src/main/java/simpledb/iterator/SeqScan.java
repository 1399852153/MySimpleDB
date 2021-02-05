package simpledb.iterator;

import simpledb.dbfile.DBFile;
import simpledb.dbrecord.Record;

/**
 * @author xiongyx
 * @date 2021/2/5
 */
public class SeqScan implements DbFileIterator<Record>{

    private DBFile dbFile;


    @Override
    public void open() {

    }

    @Override
    public void close() {

    }

    @Override
    public void reset() {

    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public Record next() {
        return null;
    }
}
