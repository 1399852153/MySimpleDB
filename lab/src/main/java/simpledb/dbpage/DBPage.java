package simpledb.dbpage;

import simpledb.dbrecord.Record;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author xiongyx
 * @date 2021/2/5
 */
public interface DBPage<T> {

    byte[] serialize() throws IOException;
    int getNotEmptySlotsNum();
    int getMaxSlotNum();
    PageId getPageId();
    Iterator<T> iterator();
    Iterator<T> reverseIterator();
}

