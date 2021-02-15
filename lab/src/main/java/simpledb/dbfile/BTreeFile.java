package simpledb.dbfile;

import simpledb.Database;
import simpledb.dbpage.DBPage;
import simpledb.dbpage.PageId;
import simpledb.dbpage.btree.*;
import simpledb.dbrecord.Record;
import simpledb.exception.DBException;
import simpledb.iterator.DbFileIterator;
import simpledb.iterator.enums.OperatorEnum;
import simpledb.matadata.fields.Field;
import simpledb.matadata.table.TableDesc;
import sun.jvm.hotspot.debugger.Page;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;

/**
 * @author xiongyx
 * @date 2021/2/15
 */
public class BTreeFile implements DBFile{

    private final File f;
    private final TableDesc tableDesc;
    private final String tableId ;
    private final int keyFieldIndex;

    public BTreeFile(File f, TableDesc tableDesc, String tableId, int keyFieldIndex) {
        this.f = f;
        this.tableDesc = tableDesc;
        this.tableId = tableId;
        this.keyFieldIndex = keyFieldIndex;
    }

    public File getF() {
        return f;
    }

    public String getTableId() {
        return tableId;
    }

    public int getKeyFieldIndex() {
        return keyFieldIndex;
    }

    @Override
    public TableDesc getTableDesc() {
        return this.tableDesc;
    }

    @Override
    public File getDbFile() {
        return this.f;
    }

    @Override
    public DBPage readPage(PageId pageId) {
        BTreePageId id = (BTreePageId) pageId;
        try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(f))) {
            if (id.getPageCategory() == BTreePageCategoryEnum.ROOT_PTR.getValue()) {
                // 根节点指针页
                byte[] pageBuf = new byte[BTreeRootPtrPage.ROOT_PTR_PAGE_SIZE];

                // 从文件的头部开始读(偏移为0)
                int retVal = bis.read(pageBuf, 0, BTreeRootPtrPage.ROOT_PTR_PAGE_SIZE);
                if (retVal == -1) {
                    throw new IllegalArgumentException("Read past end of table");
                }
                if (retVal < BTreeRootPtrPage.ROOT_PTR_PAGE_SIZE) {
                    throw new IllegalArgumentException("Unable to read "
                            + BTreeRootPtrPage.ROOT_PTR_PAGE_SIZE + " bytes from BTreeFile");
                }
                return new BTreeRootPtrPage(id, pageBuf);
            } else {
                // 非根节点指针页

                byte[] pageBuf = new byte[Database.getBufferPool().getPageSize()];
                // 跳过第一页的根指针页，再偏移pageNo-1页(-1是因为根指针页是第0页，计算后面的非根节点指针页偏移时需要排除)
                int needSkip = BTreeRootPtrPage.ROOT_PTR_PAGE_SIZE + (id.getPageNo() - 1) * Database.getBufferPool().getPageSize();
                long realSkip = bis.skip(needSkip);
                if (realSkip != needSkip) {
                    throw new DBException("Unable to seek to correct place in BTreeFile");
                }
                // 读取出对应页的数据
                int retVal = bis.read(pageBuf, 0, Database.getBufferPool().getPageSize());

                if (retVal == -1) {
                    throw new DBException("Read past end of table");
                }
                if (retVal < Database.getBufferPool().getPageSize()) {
                    throw new DBException("Unable to read "
                            + Database.getBufferPool().getPageSize() + " bytes from BTreeFile");
                }

                // 按照参数指定的页面类型，进行解析、反序列化
                if(id.getPageCategory() == BTreePageCategoryEnum.INTERNAL.getValue()) {
                    // 内部节点
                    return new BTreeInternalPage(this.tableDesc,id, pageBuf, this.keyFieldIndex);
                } else if(id.getPageCategory() == BTreePageCategoryEnum.LEAF.getValue()) {
                    // 叶子节点
                    return new BTreeLeafPage(this.tableDesc,id, pageBuf, this.keyFieldIndex);
                } else if(id.getPageCategory() == BTreePageCategoryEnum.HEADER.getValue()) {
                    // header页
                    return new BTreeHeaderPage(id, pageBuf);
                } else {
                    throw new DBException("un matched pageCategoryType=" + id.getPageCategory());
                }
            }
        } catch (IOException e) {
            throw new DBException("read BTreeFile page error",e);
        }
    }

    @Override
    public void writePage(DBPage dbPage) {
        BTreePageId id = (BTreePageId) dbPage.getPageId();

        byte[] data;
        try {
            data = dbPage.serialize();
            RandomAccessFile rf = new RandomAccessFile(f, "rw");
            if(id.getPageCategory() == BTreePageCategoryEnum.ROOT_PTR.getValue()) {
                // 根节点指针页位于文件起始处，直接写入即可
                rf.write(data);
                rf.close();
            }
            else {
                // 非根节点指针页需要skip对应的偏移量再写入
                int needSkip = BTreeRootPtrPage.ROOT_PTR_PAGE_SIZE + (dbPage.getPageId().getPageNo() - 1) * Database.getBufferPool().getPageSize();
                rf.seek(needSkip);
                rf.write(data);
                rf.close();
            }
        } catch (IOException e) {
            throw new DBException("write BTreeFile page error",e);
        }
    }


    /**
     * Returns the number of pages in this BTreeFile.
     */
    public int numPages() {
        // we only ever write full pages
        return (int) ((f.length() - BTreeRootPtrPage.ROOT_PTR_PAGE_SIZE)/ Database.getBufferPool().getPageSize());
    }



    @Override
    public DbFileIterator<Record> getIterator() {
        return null;
    }


    /**
     * 根据pid查找叶子页
     * @param f f不为null，代表查找特定f所处的叶子页；f为null，代表查找的是最左边的叶子页
     */
    private BTreeLeafPage findLeafPage(BTreePageId pid, Field f){
        // some code goes here
        if (pid.getPageCategory() == BTreePageCategoryEnum.LEAF.getValue()) {
            // 查找到了最终的叶子节点类型的页
            return (BTreeLeafPage) this.getPage(pid);
        }

        // 位于中间的内部页
        BTreePageId nextSearchId;
        BTreeInternalPage searchPg = (BTreeInternalPage) this.getPage(pid);

        BTreeEntry entry;
        Iterator<BTreeEntry> it = searchPg.iterator();
        if (it.hasNext()) {
            entry = it.next();
        } else {
            throw new DBException("findLeafPage: InternalPage must contain at least one data");
        }

        if (f == null) {
            // If f is null, it finds the left-most leaf page -- used for the iterator
            // f为null，代表查找的是最左边的叶子页
            nextSearchId = entry.getLeftChild();
        } else {
            // f不为null，是有针对性的查找对应的leafPage
            while (f.compare(OperatorEnum.GREATER_THAN, entry.getKey()) && it.hasNext()) {
                // 在当前页中迭代，尝试查找到一个比f更大的项
                entry = it.next();
            }
            // 跳出循环，说明找到了一个比f更大的项或者到了当前页的最右边

            if (f.compare(OperatorEnum.LESS_THAN_OR_EQ, entry.getKey())) {
                // f<=找到的entry.key，去左孩子节点页寻找
                nextSearchId = entry.getLeftChild();
            } else {
                // greater than the last one
                // f比当前页中最后的一项更大，去右孩子节点寻找
                nextSearchId = entry.getRightChild();
            }
        }

        // 递归查找B+树更下一层的页(nextSearchId)
        return findLeafPage(nextSearchId,f);
    }

    /**
     * 拆分叶子节点
     * */
    public BTreeLeafPage splitLeafPage(BTreeLeafPage page, Field field){
        return null;
    }

//    private Page getEmptyPage(BTreePageCategoryEnum pageCategoryEnum) {
//        // create the new page
//        int emptyPageNo = getEmptyPageNo(tid, dirtypages);
//        BTreePageId newPageId = new BTreePageId(tableid, emptyPageNo, pgcateg);
//
//        // write empty page to disk
//        RandomAccessFile rf = new RandomAccessFile(f, "rw");
//        rf.seek(BTreeRootPtrPage.getPageSize() + (emptyPageNo-1) * BufferPool.getPageSize());
//        rf.write(BTreePage.createEmptyPageData());
//        rf.close();
//
//        // make sure the page is not in the buffer pool	or in the local cache
//        Database.getBufferPool().discardPage(newPageId);
//
//        return getPage(tid, dirtypages, newPageId, Permissions.READ_WRITE);
//    }

    private BTreeRootPtrPage getRootPtrPage() throws IOException {
        synchronized(this) {
            // 如果文件整个都是空的，放并发的构造初始化的空BTreeRootPtrPage和一个空BTreeLeafPage
            if(f.length() == 0) {
                // create the root pointer page and the root page
                BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream(f, true));
                byte[] emptyRootPtrData = BTreeRootPtrPage.createEmptyPageData();
                bw.write(emptyRootPtrData);

                byte[] emptyLeafData = BTreeLeafPage.createEmptyPageData();
                bw.write(emptyLeafData);
                bw.close();
            }
        }

        // 当前文件根指针页已经存在，直接返回
        return (BTreeRootPtrPage) getPage(BTreeRootPtrPage.getId(this.tableId));
    }

    private DBPage getPage(BTreePageId pid) {
        // 从bufferPool中查找
        return Database.getBufferPool().getPage(pid);
    }

}
