package simpledb.dbpage;

import simpledb.dbrecord.Record;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author xiongyx
 * @date 2021/2/5
 */
public interface DBPage {

    byte[] serialize() throws IOException;
    void insertRecord(Record newRecord);
    void deleteRecord(Record recordNeedDelete);
    int getNotEmptySlotsNum();
    int getMaxSlotNum();
    PageId getPageId();
    Iterator<Record> iterator();
    Iterator<Record> reverseIterator();
}

