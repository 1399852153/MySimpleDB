package simpledb.iterator.operator;

import simpledb.dbfile.DBFile;
import simpledb.dbrecord.Record;
import simpledb.iterator.DbFileIterator;
import simpledb.matadata.table.TableDesc;

/**
 * @author xiongyx
 * @date 2021/2/5
 */
public class SeqScan implements DbIterator {

    private DBFile dbFile;
    private DbFileIterator<Record> dbFileIterator;

    public SeqScan(DBFile dbFile) {
        this.dbFile = dbFile;
        this.dbFileIterator = dbFile.getIterator();
    }

    @Override
    public TableDesc getTupleDesc() {
        return dbFile.getTableDesc();
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

    public void preShow(){
        this.open();
        while(this.hasNext()){
            Record record = this.next();
            System.out.println(record);
        }

        this.reset();
    }
}
