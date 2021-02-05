package simpledb.iterator;

import simpledb.dbfile.DBFile;
import simpledb.dbrecord.Record;

/**
 * @author xiongyx
 * @date 2021/2/5
 */
public class SeqScan implements DbFileIterator<Record>{

    private DBFile dbFile;
    private DbFileIterator<Record> dbFileIterator;

    public SeqScan(DBFile dbFile) {
        this.dbFile = dbFile;
        this.dbFileIterator = dbFile.getIterator();
    }

    @Override
    public void open() {
        dbFileIterator.open();
    }

    @Override
    public void close() {
        dbFileIterator.close();
    }

    @Override
    public void reset() {
        dbFileIterator.reset();
    }

    @Override
    public boolean hasNext() {
        return dbFileIterator.hasNext();
    }

    @Override
    public Record next() {
        return dbFileIterator.next();
    }
}
