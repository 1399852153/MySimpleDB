package simpledb.iterator.operator;

import simpledb.dbrecord.Record;
import simpledb.iterator.DbFileIterator;
import simpledb.iterator.DbIterator;
import simpledb.iterator.Predicate;

/**
 * @author xiongyx
 * @date 2021/2/5
 */
public class Filter implements DbFileIterator<Record> {

    private final Predicate predicate;
    private DbIterator childItr;
    private boolean open;
    private Record nextRecord;


    public Filter(Predicate predicate, DbIterator childItr) {
        this.predicate = predicate;
        this.childItr = childItr;
    }

    @Override
    public void open() {
        this.open = true;
        this.childItr.open();
    }

    @Override
    public void close() {
        this.open = false;
        this.childItr.close();
    }

    @Override
    public void reset() {
        this.childItr.reset();
    }

    @Override
    public boolean hasNext() {
        while (childItr.hasNext()) {
            Record next = childItr.next();
            if (predicate.filter(next)) {
                this.nextRecord = next;
                return true;
            }
        }
        return false;
    }

    @Override
    public Record next() {
        Record next = this.nextRecord;
        this.nextRecord = null;
        return next;
    }
}
