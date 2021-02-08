package simpledb.iterator.operator;

import simpledb.dbrecord.Record;
import simpledb.iterator.Predicate;
import simpledb.matadata.table.TableDesc;

/**
 * @author xiongyx
 * @date 2021/2/5
 */
public class Filter implements DbIterator {

    private final Predicate predicate;
    private final DbIterator childItr;
    private Record nextRecord;

    public Filter(Predicate predicate, DbIterator childItr) {
        this.predicate = predicate;
        this.childItr = childItr;
    }

    @Override
    public TableDesc getTupleDesc() {
        return childItr.getTupleDesc();
    }

    @Override
    public void open() {
        this.childItr.open();
    }

    @Override
    public void close() {
        this.childItr.close();
    }

    @Override
    public void reset() {
        this.childItr.reset();
    }

    @Override
    public boolean hasNext() {
        if(this.nextRecord != null){
            return true;
        }

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
