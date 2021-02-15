package simpledb.dbpage.btree;

import simpledb.Database;
import simpledb.dbpage.DBPage;
import simpledb.dbpage.PageCommonUtil;
import simpledb.dbpage.PageId;
import simpledb.dbrecord.Record;
import simpledb.dbrecord.RecordId;
import simpledb.exception.DBException;
import simpledb.exception.ParseException;
import simpledb.iterator.enums.OperatorEnum;
import simpledb.matadata.fields.Field;
import simpledb.matadata.fields.IntField;
import simpledb.matadata.table.TableDesc;
import simpledb.matadata.table.TableDescItem;
import simpledb.matadata.types.ColumnTypeEnum;
import simpledb.util.CommonUtil;

import java.io.*;
import java.util.*;

/**
 * @author xiongyx
 * @date 2021/2/12
 */
public class BTreeInternalPage implements DBPage {

    private final BTreePageId pageId;
    private final TableDesc tableDesc;
    private final int keyFieldIndex;

    private int parent; // parent is always internal node or 0 for root node


    private boolean[] bitMapHeaderArray;
    private Field[] keys;
    private Integer[] children;
    private final int maxSlotNum;
    private int childCategory; // either leaf or internal

    public BTreeInternalPage(TableDesc tableDesc,BTreePageId pageId, byte[] data, int keyFieldIndex) {
        this.tableDesc = tableDesc;
        this.pageId = pageId;
        this.keyFieldIndex = keyFieldIndex;
        this.maxSlotNum = getMaxSlotNum();

        try {
            deSerialize(data);
        } catch (IOException e) {
            throw new ParseException("deSerialize BTreeLeafPage error", e);
        }
    }

    /**
     * 反序列化 磁盘二进制数据->内存结构化数据
     */
    private void deSerialize(byte[] data) throws IOException {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // 从文件中读取出最开始的int数据，是为双亲节点指针
        IntField parentF = (IntField) ColumnTypeEnum.INT_TYPE.parse(dis);
        this.parent = parentF.getValue();

        // read the child page category
        this.childCategory = dis.readByte();

        // 解析所存储的位图header
        this.bitMapHeaderArray = new boolean[this.getMaxSlotNum()];
        for (int i = 0; i < bitMapHeaderArray.length; i++) {
            // 读取出后续的header位图
            bitMapHeaderArray[i] = dis.readBoolean();
        }
        // 不满一个字节的，将其跳过
        int needSkip = CommonUtil.bitCeilByte(this.maxSlotNum);
        for (int i = 0; i < needSkip; i++) {
            dis.readBoolean();
        }

        // 解析所存储的keys
        this.keys = new Field[this.maxSlotNum];
        // keys和children是交错的，keys相对要少一个
        // 逻辑视图（交错）：          物理视图（平行,k比c大1）
        //  k1 k2 k3                ** k1 k2 k3
        // c1 c2 c3 c4              c1 c2 c3 c4
        keys[0] = null;
        for(int i=1; i<keys.length; i++){
            keys[i] = readNextKey(dis,i);
        }

        // 解析所存储的children
        this.children = new Integer[this.maxSlotNum];
        for (int i=0; i<children.length; i++) {
            children[i] = readNextChild(dis, i);
        }
    }

    @Override
    public byte[] serialize() throws IOException {
        int len = Database.getBufferPool().getPageSize();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(byteArrayOutputStream);

        dos.writeInt(parent);
        dos.writeByte((byte) childCategory);

        // 写入头部位图
        for (boolean b : this.bitMapHeaderArray) {
            dos.writeBoolean(b);
        }
        // 不满一个字节的，将其跳过
        int needSkip = CommonUtil.bitCeilByte(this.maxSlotNum);
        for (int i = 0; i < needSkip; i++) {
            dos.writeBoolean(false);
        }

        // 写入keys(和初始化时一致，keys[0]为null不存储在磁盘中)
        for (int i=1; i<keys.length; i++) {
            writeNextKey(dos,i);
        }

        // 写入children
        for (int i=0; i<children.length; i++) {
            writeNextChild(dos,i);
        }

        // 如果实际不足一页，用0填充页内剩余的空间(实际数据无法和页大小恰好对齐)
        int parentPointSpace = BTreeConstants.INDEX_SIZE;
        int childCategoryToByteSpace = 1;
        int bitMapHeaderArraySpace = (this.bitMapHeaderArray.length + needSkip)/8;
        int keysSpace = this.tableDesc.getColumn(this.keyFieldIndex).getColumnTypeEnum().getLength() * (keys.length - 1);
        int childrenSpace = BTreeConstants.INDEX_SIZE * children.length;

        int needPaddingLength = Database.getBufferPool().getPageSize() -
                (parentPointSpace + childCategoryToByteSpace + bitMapHeaderArraySpace + keysSpace + childrenSpace);
        if (needPaddingLength > 0) {
            byte[] zeroes = new byte[needPaddingLength];
            // 后续空余的空间，用0补齐
            dos.write(zeroes, 0, needPaddingLength);
        }

        // 强制刷新
        dos.flush();

        return byteArrayOutputStream.toByteArray();
    }

    @Override
    public int getNotEmptySlotsNum() {
        return PageCommonUtil.getNotEmptySlotsNum(this.bitMapHeaderArray);
    }

    @Override
    public int getMaxSlotNum() {
       return getMaxEntryNum()+1;
    }

    private int getMaxEntryNum(){
        int keySize = this.tableDesc.getColumn(this.keyFieldIndex).getColumnTypeEnum().getLength();
        // 每一个key所占用的bit位+索引位+1位header位
        int bitsPerEntryIncludingHeader = keySize * 8 + BTreeConstants.INDEX_SIZE * 8 + 1;
        // extraBits are: one parent pointer, 1 byte for child page category,
        // one extra child pointer (node with m entries has m+1 pointers to children), 1 bit for extra header
        int extraBits = 2 * BTreeConstants.INDEX_SIZE * 8 + 8 + 1;
        // 每一个页所能容纳的最大entry数
        return (Database.getBufferPool().getPageSize()*8 - extraBits) / bitsPerEntryIncludingHeader;
    }

    @Override
    public PageId getPageId() {
        return this.pageId;
    }

    public BTreePageId getBTreePageId(){
        return this.pageId;
    }

    public void insertEntry(BTreeEntry e){
        // 校验start
        if (!e.getKey().getType().equals(this.tableDesc.getColumn(this.keyFieldIndex).getColumnTypeEnum())) {
            throw new DBException("key field type mismatch, in insertEntry");
        }
        if(e.getLeftChild().getTableId().equals(this.pageId.getTableId()) || e.getRightChild().getTableId().equals(this.pageId.getTableId())) {
            throw new DBException("table id mismatch in insertEntry");
        }

        if(childCategory == BTreePageCategoryEnum.ROOT_PTR.getValue()) {
            if(e.getLeftChild().getPageCategory() != e.getRightChild().getPageCategory()) {
                throw new DBException("child page category mismatch in insertEntry");
            }
            childCategory = e.getLeftChild().getPageCategory();
        }else if(e.getLeftChild().getPageCategory() != childCategory || e.getRightChild().getPageCategory() != childCategory) {
            throw new DBException("child page category mismatch in insertEntry");
        }
        // 校验end

        // 整个页的第一条entry插入
        if(getNotEmptySlotsNum() == 0) {
            children[0] = e.getLeftChild().getPageNo();
            children[1] = e.getRightChild().getPageNo();
            keys[1] = e.getKey();
            this.bitMapHeaderArray[0] = true;
            this.bitMapHeaderArray[1] = true;
            e.setRecordId(new RecordId(this.pageId, 1));
            return;
        }

        int firstEmptySlotIndex = PageCommonUtil.getFirstEmptySlotIndex(this.bitMapHeaderArray,true);
        int mostRightLessThanTargetIndex = findMostRightIndexLessThanTarget(e);

        // shift entries back or forward to fill empty slot and make room for new entry
        // while keeping entries in sorted order
        int goodSlot;
        if(firstEmptySlotIndex < mostRightLessThanTargetIndex) {
            for(int i = firstEmptySlotIndex; i < mostRightLessThanTargetIndex; i++) {
                moveEntry(i+1, i);
            }
            goodSlot = mostRightLessThanTargetIndex;
        }
        else {
            for(int i = firstEmptySlotIndex; i > mostRightLessThanTargetIndex + 1; i--) {
                moveEntry(i-1, i);
            }
            goodSlot = mostRightLessThanTargetIndex + 1;
        }

        // insert new entry into the correct spot in sorted order
        this.bitMapHeaderArray[goodSlot] = true;
        keys[goodSlot] = e.getKey();
        children[goodSlot] = e.getRightChild().getPageNo();
        e.setRecordId(new RecordId(this.pageId, goodSlot));
    }

    /**
     * Move an entry from one slot to another slot, and update the corresponding headers
     *
     * from插槽中的数据迁移进to插槽中（修改keys、children和位图header）
     */
    private void moveEntry(int from, int to) {
        if(!this.bitMapHeaderArray[to] && this.bitMapHeaderArray[from]) {
            this.bitMapHeaderArray[to] = true;
            this.bitMapHeaderArray[from] = false;

            keys[to] = keys[from];
            keys[from] = null;

            children[to] = children[from];
            children[from] = null;
        }
    }

    /**
     * 找到小于或等于参数target的最右下标
     * todo 还没理解，暂时直接整段的复制过来
     * */
    private int findMostRightIndexLessThanTarget(BTreeEntry target){
        // find the child pointer matching the left or right child in this entry
        int lessOrEqKey = -1;
        for (int i=0; i<this.maxSlotNum; i++) {
            if(this.bitMapHeaderArray[i]) {
                if(children[i] == target.getLeftChild().getPageNo() || children[i] == target.getRightChild().getPageNo()) {
                    if(i > 0 && keys[i].compare(OperatorEnum.GREATER_THAN, target.getKey())) {
                        throw new DBException("attempt to insert invalid entry with left child " +
                                target.getLeftChild().getPageNo() + ", right child " +
                                target.getRightChild().getPageNo() + " and key " + target.getKey() +
                                " HINT: one of these children must match an existing child on the page" +
                                " and this key must be correctly ordered in between that child's" +
                                " left and right keys");
                    }
                    lessOrEqKey = i;
                    if(children[i] == target.getRightChild().getPageNo()) {
                        children[i] = target.getLeftChild().getPageNo();
                    }
                }
                else if(lessOrEqKey != -1) {
                    // validate that the next key is greater than or equal to the one we are inserting
                    if(keys[i].compare(OperatorEnum.LESS_THAN, target.getKey())) {
                        throw new DBException("attempt to insert invalid entry with left child " +
                                target.getLeftChild().getPageNo() + ", right child " +
                                target.getRightChild().getPageNo() + " and key " + target.getKey() +
                                " HINT: one of these children must match an existing child on the page" +
                                " and this key must be correctly ordered in between that child's" +
                                " left and right keys");
                    }
                    break;
                }
            }
        }

        if(lessOrEqKey == -1) {
            throw new DBException("attempt to insert invalid entry with left child " +
                    target.getLeftChild().getPageNo() + ", right child " +
                    target.getRightChild().getPageNo() + " and key " + target.getKey() +
                    " HINT: one of these children must match an existing child on the page" +
                    " and this key must be correctly ordered in between that child's" +
                    " left and right keys");
        }

        return lessOrEqKey;
    }

    public void updateEntry(BTreeEntry e){
        RecordId recordId = e.getRecordId();

        // 校验start
        if(recordId == null) {
            throw new DBException("tried to update entry with null rid");
        }
        if((recordId.getPageId().getPageNo() != this.pageId.getPageNo()) || (recordId.getPageId().getTableId().equals(this.pageId.getTableId()))) {
            throw new DBException("tried to update entry on invalid page or table");
        }
        if (!this.bitMapHeaderArray[recordId.getPageInnerNo()]){
            throw new DBException("tried to update null entry.");
        }

        int pageInnerNo = recordId.getPageInnerNo();
        for(int i = pageInnerNo + 1; i < this.maxSlotNum; i++) {
            if(this.bitMapHeaderArray[i]) {
                // 校验靠右边的第一个非空的key是否不比更新的key要小
                if(keys[i].compare(OperatorEnum.LESS_THAN, e.getKey())) {
                    throw new DBException("attempt to update entry with invalid key " + e.getKey() +
                            " HINT: updated key must be less than or equal to keys on the right");
                }else{
                    // 校验通过
                    break;
                }
            }
        }
        for(int i = pageInnerNo - 1; i >= 0; i--) {
            if(this.bitMapHeaderArray[i]) {
                // 校验靠左边的第一个非空的key是否不比更新的key要大
                if(i > 0 && keys[i].compare(OperatorEnum.GREATER_THAN, e.getKey())) {
                    throw new DBException("attempt to update entry with invalid key " + e.getKey() +
                            " HINT: updated key must be greater than or equal to keys on the left");
                }else {
                    // 校验通过 更新left child
                    children[i] = e.getLeftChild().getPageNo();
                    break;
                }
            }
        }
        // 校验end

        // 更新right children和keys
        children[pageInnerNo] = e.getRightChild().getPageNo();
        keys[pageInnerNo] = e.getKey();
    }

    public void deleteKeyAndRightChild(BTreeEntry e){
        deleteEntry(e,true);
    }

    public void deleteKeyAndLeftChild(BTreeEntry e){
        deleteEntry(e,false);
    }

    private void deleteEntry(BTreeEntry e,boolean deleteRightChild){
        RecordId recordId = e.getRecordId();

        // 校验start
        if(recordId == null) {
            throw new DBException("tried to delete entry with null rid");
        }
        if((recordId.getPageId().getPageNo() != this.pageId.getPageNo()) || (recordId.getPageId().getTableId().equals(this.pageId.getTableId()))) {
            throw new DBException("tried to delete entry on invalid page or table");
        }
        if (!this.bitMapHeaderArray[recordId.getPageInnerNo()]){
            throw new DBException("tried to delete null entry.");
        }
        // 校验end

        e.setRecordId(null);
        int pageInnerNo = recordId.getPageInnerNo();
        if(deleteRightChild){
            this.bitMapHeaderArray[pageInnerNo] = false;
            this.keys[pageInnerNo] = null;
            this.children[pageInnerNo] = null;
        }else {
            // 如果需要删除的是左孩子，将BTreeEntry的右孩子迁移到左边（最靠右的非空插槽）
            for(int i = pageInnerNo - 1; i >= 0; i--){
                if(this.bitMapHeaderArray[pageInnerNo]){
                    children[i] = children[pageInnerNo];
                    this.bitMapHeaderArray[pageInnerNo] = false;
                    return;
                }
            }
        }
    }

    @Override
    public Iterator<BTreeEntry> iterator() {
        return new BTreeInternalPageItr(false);
    }

    @Override
    public Iterator<BTreeEntry> reverseIterator() {
        return new BTreeInternalPageItr(true);
    }

    /**
     * protected method used by the iterator to get the ith child page id out of this page
     * @param i - the index of the child page id
     * @return the ith child page id
     */
    private BTreePageId getChildId(int i) throws NoSuchElementException {
        if (i < 0 || i >= children.length) {
            throw new NoSuchElementException();
        }

        if(!this.bitMapHeaderArray[i]) {
            return null;
        }else{
            return new BTreePageId(this.pageId.getTableId(), children[i], childCategory);
        }
    }

    /**
     * 读取一个key项
     * */
    private Field readNextKey(DataInputStream dis, int slotIndex){
        if (!this.bitMapHeaderArray[slotIndex]) {
            // 位图显示记录不存在，文件指针直接跳过一个KeyField的长度
            TableDescItem tableDescItem = this.tableDesc.getColumn(this.keyFieldIndex);
            for (int i = 0; i < tableDescItem.getColumnTypeEnum().getLength(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty btree record");
                }
            }
            return null;
        } else {
            // 位图显示记录存在
            TableDescItem keyItem = this.tableDesc.getColumn(this.keyFieldIndex);
            return keyItem.getColumnTypeEnum().parse(dis);
        }
    }
    /**
     * 写入一个key项
     * */
    private void writeNextKey(DataOutputStream dos, int slotIndex) throws IOException {
        if (!this.bitMapHeaderArray[slotIndex]) {
            // 空的插槽，用0填充一个KeyField大小的空间
            TableDescItem tableDescItem = this.tableDesc.getColumn(this.keyFieldIndex);
            for (int j = 0; j < tableDescItem.getColumnTypeEnum().getLength(); j++) {
                dos.writeByte(0);
            }
        } else {
            this.keys[slotIndex].serialize(dos);
        }
    }

    /**
     * 读取一个child项
     * */
    private Integer readNextChild(DataInputStream dis, int slotIndex){
        if (!this.bitMapHeaderArray[slotIndex]) {
            // 位图显示记录不存在，文件指针直接跳过一个child索引的长度
            for (int i = 0; i < BTreeConstants.INDEX_SIZE; i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty btree record");
                }
            }
            return null;
        } else {
            // 位图显示记录存在
            IntField intField = (IntField) ColumnTypeEnum.INT_TYPE.parse(dis);
            return intField.getValue();
        }
    }
    /**
     * 写入一个child项
     * */
    private void writeNextChild(DataOutputStream dos, int slotIndex) throws IOException {
        if (!this.bitMapHeaderArray[slotIndex]) {
            // 空的插槽，用0填充一个child索引大小的空间
            for (int j = 0; j < BTreeConstants.INDEX_SIZE; j++) {
                dos.writeByte(0);
            }
        } else {
            dos.writeInt(children[slotIndex]);
        }
    }

    /**
     * 内部迭代器
     * */
    private class BTreeInternalPageItr implements Iterator<BTreeEntry>{
        private final Iterator<Field> keyItr;
        private final Iterator<Tuple> leftChildItr;
        private final Iterator<Tuple> rightChildItr;

        public BTreeInternalPageItr(boolean needReverse) {
            ArrayList<Field> noEmptyKeyList = new ArrayList<>();
            for (int i = 1; i < BTreeInternalPage.this.maxSlotNum; i++) {
                if (BTreeInternalPage.this.bitMapHeaderArray[i]) {
                    // 过滤掉为空的插槽
                    noEmptyKeyList.add(i, BTreeInternalPage.this.keys[i]);
                }
            }
            if(needReverse) {
                Collections.reverse(noEmptyKeyList);
            }
            keyItr = noEmptyKeyList.iterator();


            List<Tuple> noEmptyLeftChildrenList = new ArrayList<>();
            List<Tuple> noEmptyRightChildrenList = new ArrayList<>();
            for (int i = 0; i < BTreeInternalPage.this.maxSlotNum-1; i++) {
                if (BTreeInternalPage.this.bitMapHeaderArray[i]) {
                    // 过滤掉为空的插槽
                    noEmptyLeftChildrenList.add(new Tuple(BTreeInternalPage.this.children[i],i));
                    noEmptyRightChildrenList.add(new Tuple(BTreeInternalPage.this.children[i+1],i+1));
                }
            }
            if(needReverse) {
                Collections.reverse(noEmptyLeftChildrenList);
                Collections.reverse(noEmptyRightChildrenList);
            }
            this.leftChildItr = noEmptyLeftChildrenList.iterator();
            this.rightChildItr = noEmptyRightChildrenList.iterator();
        }

        @Override
        public boolean hasNext() {
            return keyItr.hasNext();
        }

        @Override
        public BTreeEntry next() {
            Field nextKey = keyItr.next();
            Tuple leftChild = leftChildItr.next();
            BTreePageId leftChildId = new BTreePageId(BTreeInternalPage.this.tableDesc.getTableId(), leftChild.getChildrenKey(), childCategory);
            Tuple rightChild = rightChildItr.next();
            BTreePageId rightChildId = new BTreePageId(BTreeInternalPage.this.tableDesc.getTableId(), rightChild.getChildrenKey(), childCategory);

            BTreeEntry nextEntry = new BTreeEntry(nextKey,leftChildId,rightChildId);
            // value为实际的页内编号
            nextEntry.setRecordId(new RecordId(BTreeInternalPage.this.pageId,rightChild.getChildIndex()));
            return nextEntry;
        }

        private class Tuple implements Comparator<Tuple>{
            private final Integer childrenKey;
            private final Integer childIndex;

            public Tuple(Integer childrenKey, Integer childIndex) {
                this.childrenKey = childrenKey;
                this.childIndex = childIndex;
            }

            public Integer getChildrenKey() {
                return childrenKey;
            }

            public Integer getChildIndex() {
                return childIndex;
            }

            @Override
            public int compare(Tuple o1, Tuple o2) {
                return o1.childIndex.compareTo(o2.childIndex);
            }
        }
    }
}
