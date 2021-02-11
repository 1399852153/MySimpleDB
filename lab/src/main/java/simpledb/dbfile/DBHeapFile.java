package simpledb.dbfile;

import simpledb.Database;
import simpledb.dbpage.normal.DBHeapPage;
import simpledb.dbpage.DBPage;
import simpledb.dbpage.normal.HeapPageId;
import simpledb.dbpage.PageId;
import simpledb.dbrecord.Record;
import simpledb.exception.DBException;
import simpledb.iterator.DbFileIterator;
import simpledb.matadata.table.TableDesc;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author xiongyx
 * @date 2021/2/2
 */
public class DBHeapFile implements DBFile{

    private final TableDesc tableDesc;
    private final File dbFile;

    public DBHeapFile(TableDesc tableDesc, File dbFile) {
        this.tableDesc = tableDesc;
        this.dbFile = dbFile;
    }

    @Override
    public TableDesc getTableDesc() {
        return tableDesc;
    }

    @Override
    public File getDbFile() {
        return dbFile;
    }

    /**
     * 读取一个页
     */
    @Override
    public DBPage readPage(PageId pageId) {
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
            return new DBHeapPage(this.tableDesc, new HeapPageId(tableId, pgNo), rawPgData);
        } catch (IOException e) {
            throw new DBException("HeapFile readPage error");
        }
    }

    /**
     * 读取一个页
     */
    @Override
    public void writePage(DBPage dbPage) {
        PageId pageId = dbPage.getPageId();
        int pgNo = pageId.getPageNo();

        final int pageSize = Database.getBufferPool().getPageSize();
        try {
            byte[] pgData = dbPage.serialize();
            RandomAccessFile randomAccessFile = new RandomAccessFile(this.dbFile, "rws");
            // 找到对应的位置
            randomAccessFile.skipBytes(pgNo * pageSize);
            // 写入整页的数据
            randomAccessFile.write(pgData);
        } catch (IOException e) {
            throw new DBException("HeapFile writePage error pageId=" + pageId);
        }
    }

    /**
     * 获得文件的迭代器
     * */
    @Override
    public DbFileIterator<Record> getIterator(){
        return new HeapFileIterator(this.tableDesc.getTableId());
    }

    /**
     * 当前文件存在多少页
     */
    private int getPageNum() {
        return (int) dbFile.length() / Database.getBufferPool().getPageSize();
    }

    // =============================== DBFile迭代器 ====================================

    private class HeapFileIterator implements DbFileIterator<Record> {

        private Integer pgCursor;
        private Iterator<Record> pageIterator;
        private final String tableId;
        private final int numPages;

        public HeapFileIterator(String tableId) {
            this.pgCursor = null;
            this.pageIterator = null;
            this.tableId = tableId;
            this.numPages = getPageNum();
        }

        @Override
        public void open() {
            pgCursor = 0;
            pageIterator = getNewPageIterator(pgCursor);
        }

        @Override
        public void close() {
            pgCursor = null;
            pageIterator = null;
        }

        @Override
        public void reset() {
            close();
            open();
        }

        @Override
        public boolean hasNext() {
            if(pageIterator == null){
                return false;
            }

            while (pgCursor < numPages - 1) {
                if(pageIterator.hasNext()){
                    return true;
                }else{
                    pgCursor += 1;
                    pageIterator = getNewPageIterator(pgCursor);
                }
            }

            return pageIterator.hasNext();
        }

        @Override
        public Record next() {
            if (hasNext())  {
                return pageIterator.next();
            }else{
                throw new NoSuchElementException("no more record");
            }
        }

        private Iterator<Record> getNewPageIterator(int pgNo) {
            HeapPageId pid = new HeapPageId(tableId, pgNo);
            return Database
                    .getBufferPool()
                    .getPage(pid).iterator();
        }
    }

}
