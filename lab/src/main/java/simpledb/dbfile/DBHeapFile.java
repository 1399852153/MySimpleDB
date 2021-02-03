package simpledb.dbfile;

import simpledb.dbpage.DBHeapPage;
import simpledb.dbpage.PageId;
import simpledb.matadata.table.TableDesc;

import java.io.File;

/**
 * @author xiongyx
 * @date 2021/2/2
 */
public class DBHeapFile {

    private TableDesc tableDesc;
    private File dbFile;


    public DBHeapPage readPage(PageId pageId){
        return null;
    }

}
