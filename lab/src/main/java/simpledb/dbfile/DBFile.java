package simpledb.dbfile;

import simpledb.dbpage.DBPage;
import simpledb.dbpage.PageId;
import simpledb.dbrecord.Record;
import simpledb.iterator.DbFileIterator;
import simpledb.matadata.table.TableDesc;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xiongyx
 * @date 2021/2/5
 */
public interface DBFile {

    TableDesc getTableDesc();
    File getDbFile();
    DBPage readPage(PageId pageId);
    void writePage(DBPage dbPage);

    List<DBPage> insertTuple(Record newRecord) throws IOException;
    List<DBPage> deleteTuple(Record recordNeedDelete);

    DbFileIterator<Record> getIterator();
}
