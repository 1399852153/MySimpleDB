package simpledb.dbfile;

import simpledb.Database;
import simpledb.dbpage.DBPage;
import simpledb.dbpage.PageCommonUtil;
import simpledb.dbpage.PageId;
import simpledb.dbpage.btree.*;
import simpledb.dbrecord.Record;
import simpledb.exception.DBException;
import simpledb.iterator.DbFileIterator;
import simpledb.iterator.enums.OperatorEnum;
import simpledb.matadata.fields.Field;
import simpledb.matadata.table.TableDesc;

import java.io.*;
import java.util.*;

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

    @Override
    public List<DBPage> insertTuple(Record newRecord) throws IOException {
        HashMap<PageId, DBPage> dirtyPages = new HashMap<>();

        BTreeRootPtrPage rootPtr = getRootPtrPage(dirtyPages);
        BTreePageId rootPtrId = rootPtr.getRootId();

        if(rootPtrId == null) {
            // 根节点指针页当前还不存在，构建一个BTreeRootPtrPage
            rootPtr = (BTreeRootPtrPage) getPage(dirtyPages,BTreeRootPtrPage.getId(this.tableId));
            rootPtrId = new BTreePageId(this.tableId, 0, BTreePageCategoryEnum.LEAF.getValue());
            rootPtr.setRootId(rootPtrId);
        }

        Field targetField = newRecord.getField(this.keyFieldIndex);
        // 查询需要插入的目标叶子页
        BTreeLeafPage targetLeafPage = findLeafPage(dirtyPages,rootPtrId,targetField);
        if(targetLeafPage.getNotEmptySlotsNum() == targetLeafPage.getMaxSlotNum()) {
            // 叶子页空间不足，进行拆分
            targetLeafPage = splitLeafPage(dirtyPages,targetLeafPage,targetField);
        }
        targetLeafPage.insertRecord(newRecord);

        return new ArrayList<>(dirtyPages.values());
    }

    @Override
    public List<DBPage> deleteTuple(Record recordNeedDelete) {
        HashMap<PageId, DBPage> dirtyPages = new HashMap<>();

        PageId targetPageId = recordNeedDelete.getRecordId().getPageId();
        BTreePageId pageId = new BTreePageId(this.tableId, targetPageId.getPageNo(), BTreePageCategoryEnum.LEAF.getValue());
        // 找到目标叶子页，删除记录
        BTreeLeafPage targetLeafPage = (BTreeLeafPage) getPage(dirtyPages,pageId);
        targetLeafPage.deleteRecord(recordNeedDelete);

        int lowThreshold = PageCommonUtil.lowThreshold(targetLeafPage);
        if(targetLeafPage.getNotEmptySlotsNum() < lowThreshold){
            // 当被删除数据的页中数据小于阈值时（小于最大容纳数据量的一半）
            // 需要视情况合并相邻的兄弟页节点，或是从负载较高的兄弟页中挪动一部分到当前负载不足的页（总之就是保证空间、时间效率的平衡）
            handleMinOccupancyPage(dirtyPages,targetLeafPage);
        }

        return new ArrayList<>(dirtyPages.values());
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
    private BTreeLeafPage findLeafPage(HashMap<PageId, DBPage> dirtyPages,BTreePageId pid, Field f){
        // some code goes here
        if (pid.getPageCategory() == BTreePageCategoryEnum.LEAF.getValue()) {
            // 查找到了最终的叶子节点类型的页
            return (BTreeLeafPage) this.getPage(dirtyPages,pid);
        }

        // 位于中间的内部页
        BTreePageId nextSearchId;
        BTreeInternalPage searchPg = (BTreeInternalPage) this.getPage(dirtyPages,pid);

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
        return findLeafPage(dirtyPages,nextSearchId,f);
    }

    /**
     * 拆分叶子节点页
     * @return 返回拆分后，field最终被插入的那个叶子页
     * */
    private BTreeLeafPage splitLeafPage(HashMap<PageId, DBPage> dirtyPages, BTreeLeafPage leafPageNeedSplit, Field field) throws IOException {
        // 拆分叶子结点页平摊所存储的数据，先创建一个右兄弟页
        BTreeLeafPage newRightSib = (BTreeLeafPage) getEmptyPage(dirtyPages,BTreePageCategoryEnum.LEAF);

        // 将需要从原来页面拆分出来的记录暂时收集到recordToMove中
        Iterator<Record> it = leafPageNeedSplit.reverseIterator();
        Record[] recordToMove = new Record[(leafPageNeedSplit.getNotEmptySlotsNum()+1) / 2];
        int moveCnt = recordToMove.length - 1;
        while (moveCnt >= 0 && it.hasNext()) {
            recordToMove[moveCnt--] = it.next();
        }
        // 从原页面中删除，在新页面中插入
        for (int i = recordToMove.length-1; i >= 0; --i) {
            leafPageNeedSplit.deleteRecord(recordToMove[i]);
            newRightSib.insertRecord(recordToMove[i]);
        }

        // 被拆分出来的新叶子页面，其最小的记录作为key（新页面是右节点）
        Field midKey = recordToMove[0].getField(this.keyFieldIndex);
        // 获得被拆分叶子节点页的双亲页
        BTreeInternalPage parent = getParentWithEmptySlots(dirtyPages,leafPageNeedSplit, midKey);

        BTreePageId oldRightSibId = leafPageNeedSplit.getRightSiblingId();
        // 新创建的newRightSib位于被拆分页面page和page之前的oldRightSibId之间（page<=>newRightSib<=>oldRightSibId）
        newRightSib.setRightSiblingId(oldRightSibId);
        newRightSib.setLeftSiblingId(leafPageNeedSplit.getBTreePageId());
        leafPageNeedSplit.setRightSiblingId(newRightSib.getBTreePageId());
        if (oldRightSibId != null) {
            BTreeLeafPage oldRightSib = (BTreeLeafPage) getPage(dirtyPages,leafPageNeedSplit.getRightSiblingId());
            oldRightSib.setLeftSiblingId(newRightSib.getBTreePageId());
            // 拆分叶子页时oldRightSibId存在，存入dirtyPages
            dirtyPages.put(oldRightSib.getPageId(),oldRightSib);
        }
        // 拆分叶子页时，parent、leafPageNeedSplit、newRightSib都被修改过了，存入dirtyPages
        dirtyPages.put(newRightSib.getPageId(),newRightSib);
        dirtyPages.put(parent.getPageId(),parent);
        dirtyPages.put(leafPageNeedSplit.getPageId(),leafPageNeedSplit);

        // 设置两者共同的双亲
        newRightSib.setParentId(parent.getBTreePageId());
        leafPageNeedSplit.setParentId(parent.getBTreePageId());

        // 拆分后，parent双亲页中插入新的一条BTreeEntry
        BTreeEntry newParentEntry = new BTreeEntry(midKey, leafPageNeedSplit.getBTreePageId(), newRightSib.getBTreePageId());
        parent.insertEntry(newParentEntry);

        // 返回的是拆分后，field最终被插入的那个叶子页
        if (field.compare(OperatorEnum.GREATER_THAN, midKey)) {
            // field>midKey,field被插入在newRightSib页中
            return newRightSib;
        } else {
            // field<=midKey,field被插入在page页中
            return leafPageNeedSplit;
        }
    }

    /**
     * 拆分内部节点页
     * @return 返回拆分后，field最终被插入的那个内部节点页
     * */
    private BTreeInternalPage splitInternalPage(HashMap<PageId, DBPage> dirtyPages,BTreeInternalPage internalPageNeedSplit, Field field) throws IOException {
        // 拆分内部结点页平摊所存储的数据，先创建一个右兄弟页
        BTreeInternalPage newRightSib = (BTreeInternalPage) getEmptyPage(dirtyPages,BTreePageCategoryEnum.INTERNAL);

        // 将需要从原来页面拆分出来的记录暂时收集到entryToMove中
        Iterator<BTreeEntry> it = internalPageNeedSplit.reverseIterator();
        BTreeEntry[] entryToMove = new BTreeEntry[(internalPageNeedSplit.getNotEmptySlotsNum()+1) / 2];
        int moveCnt = entryToMove.length - 1;
        while (moveCnt >= 0 && it.hasNext()) {
            entryToMove[moveCnt--] = it.next();
        }

        // 从原页面中删除，在新页面中插入
        for (int i = entryToMove.length-1; i >= 0; --i) {
            if(i == 0){
                // internal页面的第0项entry比较特殊，newRightSib不需要插入
                internalPageNeedSplit.deleteKeyAndRightChild(entryToMove[0]);
            }else{
                internalPageNeedSplit.deleteKeyAndRightChild(entryToMove[i]);
                newRightSib.insertEntry(entryToMove[i]);
            }
            // 更新每一个被迁移的右孩子的parent，令newRightSib为新的parent
            updateParentPointer(dirtyPages,newRightSib.getBTreePageId(), entryToMove[i].getRightChild());
        }

        // 被拆分出来的新叶子页面，其最小的记录作为key（新页面是右节点）
        BTreeEntry midKey = entryToMove[0];
        // midKey设置左右孩子
        midKey.setLeftChild(internalPageNeedSplit.getBTreePageId());
        midKey.setRightChild(newRightSib.getBTreePageId());

        // 拆分后，parent双亲页中插入新的一条BTreeEntry
        BTreeInternalPage parent = getParentWithEmptySlots(dirtyPages,internalPageNeedSplit, midKey.getKey());
        parent.insertEntry(midKey);

        // 设置被拆分的两个内部节点页的双亲
        internalPageNeedSplit.setParentId(parent.getBTreePageId());
        newRightSib.setParentId(parent.getBTreePageId());

        // 拆分内部页时，parent、leafPageNeedSplit、newRightSib都被修改过了，存入dirtyPages
        dirtyPages.put(internalPageNeedSplit.getPageId(), internalPageNeedSplit);
        dirtyPages.put(newRightSib.getPageId(), newRightSib);
        dirtyPages.put(parent.getPageId(), parent);

        // 返回的是拆分后，field最终被插入的那个内部节点页
        if (field.compare(OperatorEnum.GREATER_THAN, midKey.getKey())) {
            // field>midKey,field被插入在newRightSib页中
            return newRightSib;
        } else {
            // field<=midKey,field被插入在page页中
            return internalPageNeedSplit;
        }
    }

    /**
     * b+树节点存储数据低于阈值时的处理
     * */
    private void handleMinOccupancyPage(HashMap<PageId, DBPage> dirtyPages, BTreePage targetBTreePage){
        BTreePageId parentId = targetBTreePage.getParentId();
        BTreeEntry leftEntry = null;
        BTreeEntry rightEntry = null;
        BTreeInternalPage parent = null;

        if (parentId.getPageCategory() != BTreePageCategoryEnum.ROOT_PTR.getValue()){
            parent = (BTreeInternalPage) getPage(dirtyPages, parentId);

            Iterator<BTreeEntry> itr = parent.iterator();
            // 迭代双亲节点页，找到targetBTreePage相邻的左右兄弟
            while(itr.hasNext()) {
                BTreeEntry e = itr.next();
                if(e.getLeftChild().equals(targetBTreePage.getPageId())) {
                    rightEntry = e;
                    break;
                }
                else if(e.getRightChild().equals(targetBTreePage.getPageId())) {
                    leftEntry = e;
                }
            }
        }

        if(targetBTreePage.getBTreePageId().getPageCategory() == BTreePageCategoryEnum.LEAF.getValue()) {
            // b+树节点存储数据低于阈值时的处理(叶子节点页)
            handleMinOccupancyLeafPage(dirtyPages,(BTreeLeafPage) targetBTreePage, parent, leftEntry, rightEntry);
        }else if (targetBTreePage.getBTreePageId().getPageCategory() == BTreePageCategoryEnum.INTERNAL.getValue()){
            // b+树节点存储数据低于阈值时的处理(内部节点页)
            handleMinOccupancyInternalPage(dirtyPages, (BTreeInternalPage) targetBTreePage, parent, leftEntry, rightEntry);
        } else {
            throw new DBException("un support pageCategory:" + targetBTreePage.getBTreePageId().getPageCategory());
        }
    }

    /**
     * b+树节点存储数据低于阈值时的处理(叶子节点页)
     * */
    private void handleMinOccupancyLeafPage(HashMap<PageId, DBPage> dirtyPages, BTreeLeafPage targetBTreeLeafPage,
                                            BTreeInternalPage parent, BTreeEntry leftEntry, BTreeEntry rightEntry){
        BTreePageId leftSiblingId = leftEntry != null ? leftEntry.getLeftChild() : null;
        BTreePageId rightSiblingId = rightEntry != null ? rightEntry.getRightChild() : null;

        int lowThreshold = PageCommonUtil.lowThreshold(targetBTreeLeafPage);
        if(leftSiblingId != null){
            BTreeLeafPage leftSiblingPage = (BTreeLeafPage) getPage(dirtyPages, leftSiblingId);

            if(leftSiblingPage.getNotEmptySlotsNum() < lowThreshold) {
                // leftSiblingPage左兄弟页的空插槽数低于阈值，将左兄弟页和targetBTreeLeafPage合并为一个页
                // mergeLeafPages(tid, dirtypages, leftSibling, page, parent, leftEntry);
            }else{
                // leftSiblingPage左兄弟页的空插槽数高于阈值，从左兄弟页迁移一些数据到targetBTreeLeafPage，分摊数据
                // stealFromLeafPage(page, leftSibling, parent, leftEntry, false);
            }

        } else if(rightSiblingId != null){
            BTreeLeafPage rightSiblingPage = (BTreeLeafPage) getPage(dirtyPages, rightSiblingId);

            if(rightSiblingPage.getNotEmptySlotsNum() < lowThreshold) {
                // rightSiblingPage右兄弟页的空插槽数低于阈值，将右兄弟页和targetBTreeLeafPage合并为一个页
                // mergeLeafPages(tid, dirtypages, page, rightSibling, parent, rightEntry);
            }else{
                // rightSiblingPage右兄弟页的空插槽数高于阈值，从右兄弟页迁移一些数据到targetBTreeLeafPage，分摊数据
                // stealFromLeafPage(page, rightSibling, parent, rightEntry, true);
            }
        }
    }

    /**
     * b+树节点存储数据低于阈值时的处理(内部节点页)
     * */
    private void handleMinOccupancyInternalPage(HashMap<PageId, DBPage> dirtyPages, BTreeInternalPage page,
                                            BTreeInternalPage parent, BTreeEntry leftEntry, BTreeEntry rightEntry){

    }

    /**
     * 设置child页的双亲页id
     * */
    private void updateParentPointer(HashMap<PageId, DBPage> dirtyPages,BTreePageId parentId, BTreePageId child){
        // 先以只读的权限，查询出child对应的页
        BTreePage p = (BTreePage)getPage(dirtyPages,child);

        if(!p.getParentId().equals(parentId)) {
            // 如果child对应的页和参数pid不一致，将其parentId设置为pid
            p = (BTreePage) getPage(dirtyPages,child);
            p.setParentId(parentId);
        }
    }

    /**
     * 获得对应页的双亲页（确保返回的双亲页必定存在空插槽，否则函数内部会对当前的双亲节点进行拆分）
     * 注意：内部节点的拆分是可能递归，极端情况下可能从最下层蔓延至根节点
     * */
    private BTreeInternalPage getParentWithEmptySlots(HashMap<PageId, DBPage> dirtyPages,BTreePage bTreePage, Field field) throws IOException {
        BTreePageId parentPageId = bTreePage.getParentId();

        if(parentPageId.getPageCategory() == BTreePageCategoryEnum.ROOT_PTR.getValue()){
            // 如果对应的双亲页就是根指针页，则创建一个空的Internal内部页作为其双亲页(此时的B+树还很小)
            BTreeInternalPage newParent = (BTreeInternalPage) getEmptyPage(dirtyPages,BTreePageCategoryEnum.INTERNAL);

            // 更新根节点指针，令根节点指针指向新创建出的Internal内部页
            BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) getPage(dirtyPages,BTreeRootPtrPage.getId(this.tableId));
            rootPtr.setRootId((BTreePageId) newParent.getPageId());

            return newParent;
        }else{
            BTreeInternalPage parent = (BTreeInternalPage) getPage(dirtyPages,parentPageId);

            // split the parent if needed
            if(parent.getNotEmptySlotsNum() == parent.getNotEmptySlotsNum()) {
                // 当前双亲节点已经没有空插槽了，将双亲节点进行拆分
                parent = splitInternalPage(dirtyPages,parent, field);
            }

            // 返回双亲节点，且其一定存在空插槽
            return parent;
        }
    }

    private DBPage getEmptyPage(HashMap<PageId, DBPage> dirtyPages, BTreePageCategoryEnum pageCategoryEnum) throws IOException {
        // create the new page
        int emptyPageNo = getEmptyPageNo(dirtyPages);
        BTreePageId newPageId = new BTreePageId(this.tableId, emptyPageNo, pageCategoryEnum.getValue());

        // write empty page to disk
        RandomAccessFile rf = new RandomAccessFile(f, "rw");

        int needSkip = BTreeRootPtrPage.ROOT_PTR_PAGE_SIZE + (emptyPageNo-1) * Database.getBufferPool().getPageSize();
        rf.seek(needSkip);
        rf.write(PageCommonUtil.createEmptyPageData());
        rf.close();

        // make sure the page is not in the buffer pool	or in the local cache
        Database.getBufferPool().discardPage(newPageId);
        // 新的页面，将其从dirtyPages排除掉
        DBPage dbPage = dirtyPages.remove(newPageId);
        System.out.println("getEmptyPage: remove new dbPage=" + dbPage);

        return getPage(dirtyPages,newPageId);
    }

    private int getEmptyPageNo(HashMap<PageId, DBPage> dirtyPages) throws IOException {
        BTreeRootPtrPage rootPtr = getRootPtrPage(dirtyPages);
        BTreePageId headerId = rootPtr.getHeaderId();

        // 当前文件存在header文件
        if(headerId != null) {
            BTreeHeaderPage headerPage = (BTreeHeaderPage) getPage(dirtyPages,headerId);
            int headerPageCount = 0;
            // 尝试找到一个存在空插槽的header页，用于定位一个的空页
            // try to find a header page with an empty slot

            while(headerPage != null && headerPage.getFirstEmptySlotIndex() != -1){
                // 当headerPage不存在空插槽时，跳转到当前header页关联的下一个header页中
                headerId = headerPage.getNextPageId();

                if(headerId == null) {
                    // 如果headerId为null，代表不存在后继的header页了，跳出循环
                    headerPage = null;
                    break;
                }

                headerPage = (BTreeHeaderPage) getPage(dirtyPages,headerId);
                headerPageCount++;
            }

            // 在headerPage中找到了一个拥有空插槽的header页，从中获取空页号
            if(headerPage != null) {
                headerPage = (BTreeHeaderPage) getPage(dirtyPages,headerId);
                int emptySlot = headerPage.getFirstEmptySlotIndex();
                // getEmptyPageNo用于获取一个空页使用，因此在这里提前设置为已使用
                headerPage.markSlotUsed(emptySlot);
                // 因为headerPage中的每一个插槽对应一个页
                // 最终找到的空页号emptyPageNo由顺序遍历的header页数*每一页的最大插槽数 + 当前header页内偏移量计算而来
                return headerPageCount * headerPage.getMaxSlotNum() + emptySlot;
            }
        }

        // 当前并不存在可用的空页
        synchronized(this) {
            // 在当前BTreeFile的末尾追加和创建一个新的页
            BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream(f, true));
            byte[] emptyData = PageCommonUtil.createEmptyPageData();
            bw.write(emptyData);
            bw.close();
            // 末尾追加了一个新的空页后，返回当前文件的最后一页的页号
            return numPages();
        }
    }

    private BTreeRootPtrPage getRootPtrPage(HashMap<PageId, DBPage> dirtyPages) throws IOException {
        synchronized(this) {
            // 如果文件整个都是空的，放并发的构造初始化的空BTreeRootPtrPage和一个空BTreeLeafPage
            if(f.length() == 0) {
                // create the root pointer page and the root page
                BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream(f, true));
                byte[] emptyRootPtrData = BTreeRootPtrPage.createEmptyPageData();
                bw.write(emptyRootPtrData);

                byte[] emptyLeafData = PageCommonUtil.createEmptyPageData();
                bw.write(emptyLeafData);
                bw.close();
            }
        }

        // 当前文件根指针页已经存在，直接返回
        return (BTreeRootPtrPage) getPage(dirtyPages,BTreeRootPtrPage.getId(this.tableId));
    }

    private DBPage getPage(HashMap<PageId, DBPage> dirtyPages,BTreePageId pid) {
        if(dirtyPages.containsKey(pid)) {
            // 尝试在脏页中找到了对应的page页
            return dirtyPages.get(pid);
        }

        // 从bufferPool中查找
        return Database.getBufferPool().getPage(pid);
    }

}
