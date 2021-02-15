package simpledb.dbpage;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author xiongyx
 * @date 2021/2/5
 */
public interface DBPage {

    byte[] serialize() throws IOException;
    int getNotEmptySlotsNum();
    int getMaxSlotNum();
    PageId getPageId();
    Iterator iterator();
    Iterator reverseIterator();
}

