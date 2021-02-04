package simpledb.dbfile;

import simpledb.BufferPool;
import simpledb.Database;
import simpledb.dbpage.DBHeapPage;
import simpledb.dbpage.PageId;
import simpledb.exception.DBException;
import simpledb.matadata.table.TableDesc;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author xiongyx
 * @date 2021/2/2
 */
public class DBHeapFile {

    private final TableDesc tableDesc;
    private final File dbFile;

    public DBHeapFile(TableDesc tableDesc, File dbFile) {
        this.tableDesc = tableDesc;
        this.dbFile = dbFile;
    }

    public TableDesc getTableDesc() {
        return tableDesc;
    }

    public File getDbFile() {
        return dbFile;
    }

    /**
     * 读取一个页
     * */
    public DBHeapPage readPage(PageId pageId){
        String tableId = pageId.getTableId();
        int pgNo = pageId.getPageNo();
        final int pageSize = Database.getBufferPool().getPageSize();
        byte[] rawPgData = new byte[pageSize];

        try (FileInputStream in = new FileInputStream(dbFile)) {
            // 按照页号和页大小跳过对应的内容
            in.skip((long) pgNo * pageSize);
            // 将二进制数据读取出来
            in.read(rawPgData);
            // 将二进制数据转换为DBHeapPage
            return new DBHeapPage(this.tableDesc, new PageId(tableId, pgNo), rawPgData);
        } catch (IOException e) {
            throw new DBException("HeapFile readPage error");
        }
    }

    /**
     * 读取一个页
     * */
    public void writePage(DBHeapPage dbHeapPage) {
        // some code goes here
        // not necessary for lab1
        PageId pageId = dbHeapPage.getPageId();
        String tableId = pageId.getTableId();
        int pgNo = pageId.getPageNo();

        final int pageSize = Database.getBufferPool().getPageSize();
        try {
            byte[] pgData = dbHeapPage.serialize();
            RandomAccessFile randomAccessFile = new RandomAccessFile(this.dbFile, "rws");
            // 找到对应的位置
            randomAccessFile.skipBytes(pgNo * pageSize);
            // 写入整页的数据
            randomAccessFile.write(pgData);
        } catch (IOException e) {
            throw new DBException("HeapFile writePage error pageId=" + pageId);
        }
    }
}
