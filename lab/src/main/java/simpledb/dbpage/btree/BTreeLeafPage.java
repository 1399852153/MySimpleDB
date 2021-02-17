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
import simpledb.matadata.types.ColumnTypeEnum;
import simpledb.util.CommonUtil;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author xiongyx
 * @date 2021/2/11
 *
 * b+树叶子结点
 */
public class BTreeLeafPage extends BTreePage {

    private final int keyFieldIndex;

    /**
     * 头部位图
     */
    private boolean[] bitMapHeaderArray;

    /**
     * 所保存的数组列表
     */
    private Record[] recordArray;

    /**
     * 最大插槽数目
     */
    private final int maxSlotNum;

    /**
     * 左兄弟叶子节点页指针 0代表null
     */
    private int leftSibling;

    /**
     * 有兄弟叶子节点页指针 0代表null
     */
    private int rightSibling;

    public BTreeLeafPage(TableDesc tableDesc, BTreePageId pageId, byte[] data, int keyFieldIndex) {
        super(tableDesc,pageId);
        this.keyFieldIndex = keyFieldIndex;
        this.maxSlotNum = this.getMaxSlotNum();

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

        {
            // 从文件中读取出最开始的int数据，是为双亲节点指针
            IntField parentF = (IntField) ColumnTypeEnum.INT_TYPE.parse(dis);
            this.parent = parentF.getValue();
        }
        {
            // 从文件中读取出第二个int数据，是为左兄弟页指针
            IntField leftSiblingF = (IntField) ColumnTypeEnum.INT_TYPE.parse(dis);
            this.leftSibling = leftSiblingF.getValue();
        }
        {
            // 从文件中读取出第三个int数据，是为右兄弟页指针
            IntField rightSiblingF = (IntField) ColumnTypeEnum.INT_TYPE.parse(dis);
            this.rightSibling = rightSiblingF.getValue();
        }

        this.bitMapHeaderArray = new boolean[this.getMaxSlotNum()];
        for (int i = 0; i < bitMapHeaderArray.length; i++) {
            // 读取出后续的header位图
            bitMapHeaderArray[i] = dis.readBoolean();
        }

        // 解析所存储的业务数据
        recordArray = new Record[this.getMaxSlotNum()];
        for (int i = 0; i < recordArray.length; i++) {
            // 读取并解析出tuple数据，加入tuples中
            recordArray[i] = readNextRecord(dis, i);
        }

        dis.close();
    }

    @Override
    public byte[] serialize() throws IOException {
        int len = Database.getBufferPool().getPageSize();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(byteArrayOutputStream);

        // 首先写入parent、左/右兄弟三个指针
        dos.writeInt(this.parent);
        dos.writeInt(this.leftSibling);
        dos.writeInt(this.rightSibling);

        // 写入头部位图
        for (boolean b : this.bitMapHeaderArray) {
            dos.writeBoolean(b);
        }

        // 写入record
        for (int i = 0; i < this.recordArray.length; i++) {
            writeNextRecord(dos, i);
        }

        // 如果实际不足一页，用0填充页内剩余的空间(实际数据无法和页大小恰好对齐)
        // 字节为单位 (页大小 - (位图大小bit + record长度 * record数量）)
        int needPaddingLength = Database.getBufferPool().getPageSize() -
                ((this.bitMapHeaderArray.length) + this.tableDesc.getSize() * this.recordArray.length);
        if (needPaddingLength > 0) {
            byte[] zeroes = new byte[needPaddingLength];
            // 后续空余的空间，用0补齐
            dos.write(zeroes, 0, needPaddingLength);
        }

        // 强制刷新
        dos.flush();

        return byteArrayOutputStream.toByteArray();
    }

    public void insertRecord(Record newRecord) {
        if (!newRecord.getTableDesc().equals(tableDesc)) {
            throw new DBException("type mismatch, in addTuple");
        }

        // 找到一个空插槽
        int emptySlotIndex = PageCommonUtil.getFirstEmptySlotIndex(this.bitMapHeaderArray,true);
        // 由于B+树是有序的，找到当前页中小于或等于新增节点的最右下标
        int targetIndex = findMostRightIndexLessThanTarget(newRecord);

        // shift records back or forward to fill empty slot and make room for new record
        // while keeping records in sorted order
        int finallySlotIndex;
        if (emptySlotIndex < targetIndex) {
            // 空插槽对应的下标 < 匹配到的最右下标
            for (int i = emptySlotIndex; i < targetIndex; i++) {
                // 之间的数据进行向左的平移整理（下标i+1已使用，i未使用则向左平移一位）
                moveRecord(i + 1, i);
            }
            // 找到可以放下当前数据的插槽
            finallySlotIndex = targetIndex;
        } else {
            // 空插槽对应的下标 > 匹配到的最右下标
            for (int i = emptySlotIndex; i > targetIndex + 1; i--) {
                // 之间的数据进行向右的平移整理（下标i-1已使用，i未使用则向右平移一位）
                moveRecord(i - 1, i);
            }
            // 找到可以放下当前数据的插槽
            finallySlotIndex = targetIndex + 1;
        }

        this.bitMapHeaderArray[finallySlotIndex] = true;
        newRecord.setRecordId(new RecordId(pageId, finallySlotIndex));
        this.recordArray[finallySlotIndex] = newRecord;
    }

    public void deleteRecord(Record recordNeedDelete) {
        RecordId recordId = recordNeedDelete.getRecordId();
        if (recordId == null) {
            throw new DBException("tried to delete tuple with null rid");
        }
        if ((recordId.getPageId().getPageNo() != this.pageId.getPageNo())
                || (recordNeedDelete.getTableDesc() != this.tableDesc)) {
            throw new DBException("tried to delete tuple on invalid page or table");
        }
        if (!this.bitMapHeaderArray[recordId.getPageInnerNo()]) {
            throw new DBException("tried to delete null tuple.");
        }

        // 删除tuple时，将header对应位置空
        this.bitMapHeaderArray[recordId.getPageInnerNo()] = false;
        this.recordArray[recordId.getPageInnerNo()] = null;
        // recordId置空
        recordNeedDelete.setRecordId(null);
    }

    @Override
    public int getNotEmptySlotsNum() {
        return PageCommonUtil.getNotEmptySlotsNum(this.bitMapHeaderArray);
    }

    @Override
    public int getMaxSlotNum() {
        // td.getSize() * 8 = 每一个tuple的bit位(Byte数 * 8) + 1Byte的header位图 => 每一个Tuple占用的空间
        int bitsPerTupleIncludingHeader = this.tableDesc.getSize() * 8 + 8;
        // extraBits are: left sibling pointer, right sibling pointer, parent pointer
        // 3 * INDEX_SIZE * 8 = 三个指针(左、右兄弟以及双亲节点指针)占据的bit数(Byte数 * 8)
        int extraBits = 3 * BTreeConstants.INDEX_SIZE * 8;
        // BTreeLeafPage页面可以容纳的最大插槽数 = 缓冲页面大小 - 额外的extraBits/每一个Tuple占用的空间
        return (Database.getBufferPool().getPageSize() * 8 - extraBits) / bitsPerTupleIncludingHeader;
    }

    @Override
    public BTreePageId getPageId() {
        return this.pageId;
    }

    @Override
    public Iterator<Record> iterator() {
        return new BTreeLeafPageItr(false);
    }

    @Override
    public Iterator<Record> reverseIterator() {
        return new BTreeLeafPageItr(true);
    }


    public BTreePageId getLeftSiblingId() {
        if(leftSibling == 0) {
            return null;
        }
        return new BTreePageId(this.tableDesc.getTableId(), leftSibling, BTreePageCategoryEnum.LEAF.getValue());
    }

    public void setLeftSiblingId(BTreePageId id){
        if(id == null) {
            leftSibling = 0;
        }
        else {
            if(!id.getTableId().equals(this.tableDesc.getTableId())) {
                throw new DBException("table id mismatch in setLeftSiblingId");
            }
            if(id.getPageCategory() != BTreePageCategoryEnum.LEAF.getValue()) {
                throw new DBException("leftSibling must be a leaf node");
            }
            leftSibling = id.getPageCategory();
        }
    }

    public BTreePageId getRightSiblingId() {
        if(rightSibling == 0) {
            return null;
        }
        return new BTreePageId(this.tableDesc.getTableId(), rightSibling, BTreePageCategoryEnum.LEAF.getValue());
    }

    public void setRightSiblingId(BTreePageId id){
        if(id == null) {
            rightSibling = 0;
        }
        else {
            if(!id.getTableId().equals(this.tableDesc.getTableId())) {
                throw new DBException("table id mismatch in setRightSiblingId");
            }
            if(id.getPageCategory() != BTreePageCategoryEnum.LEAF.getValue()) {
                throw new DBException("rightSibling must be a leaf node");
            }
            rightSibling = id.getPageNo();
        }
    }

    private Record readNextRecord(DataInputStream dis, int slotIndex) {
        if (!this.bitMapHeaderArray[slotIndex]) {
            // 位图显示记录不存在，文件指针直接跳过一个record的长度
            for (int i = 0; i < this.tableDesc.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty btree record");
                }
            }
            return null;
        } else {
            // 位图显示记录存在
            Record record = new Record(this.tableDesc);
            record.setRecordId(new RecordId(this.pageId, slotIndex));

            List<ColumnTypeEnum> columnTypeEnumList = this.tableDesc.getColumnTypeEnumList();
            // 按照表结构定义，读取出每一个字段的数据
            List<Field> fieldList = columnTypeEnumList.stream()
                    .map(item -> item.parse(dis))
                    .collect(Collectors.toList());

            record.setFieldList(fieldList);

            return record;
        }
    }

    private void writeNextRecord(DataOutputStream dos, int slotIndex) throws IOException {
        if (!this.bitMapHeaderArray[slotIndex]) {
            // 空的插槽，用0填充一个record大小的空间
            for (int j = 0; j < this.tableDesc.getSize(); j++) {
                dos.writeByte(0);
            }
        } else {
            // 非空插槽，写入数据

            Record recordItem = this.recordArray[slotIndex];
            // 按照表结构定义，向dos流按照顺序写出每一个字段的数据
            recordItem.getFieldList().forEach(
                    item -> item.serialize(dos)
            );
        }
    }

    /**
     * 找到小于或等于参数target的最右下标
     */
    private int findMostRightIndexLessThanTarget(Record target) {
        Field keyField = target.getField(this.keyFieldIndex);

        int mostRightIndexLessThanTarget = -1;
        // 找到小于或等于参数target的最右下标
        for (int i = 0; i < this.maxSlotNum; i++) {
            if (this.bitMapHeaderArray[i]) {
                Field fieldItem = this.recordArray[i].getField(this.keyFieldIndex);
                if (fieldItem.compare(OperatorEnum.LESS_THAN_OR_EQ, keyField)) {
                    mostRightIndexLessThanTarget = i;
                } else {
                    return mostRightIndexLessThanTarget;
                }
            }
        }

        return mostRightIndexLessThanTarget;
    }

    /**
     * from插槽中的数据迁移进to插槽中（修改recordArray和位图header）
     */
    private void moveRecord(int from, int to) {
        // 必须from不为空有数据且to为空无数据
        if (!this.bitMapHeaderArray[to] && this.bitMapHeaderArray[from]) {
            this.bitMapHeaderArray[to] = true;
            this.bitMapHeaderArray[from] = false;

            Record fromRecord = this.recordArray[from];
            fromRecord.setRecordId(new RecordId(this.pageId, to));
            this.recordArray[to] = fromRecord;
            this.recordArray[from] = null;
        }
    }

    private class BTreeLeafPageItr implements Iterator<Record> {
        private final Iterator<Record> iter;

        public BTreeLeafPageItr(boolean isReverse) {
            ArrayList<Record> noEmptyRecordArrayList = new ArrayList<>();
            for (int i = 0; i < BTreeLeafPage.this.maxSlotNum; i++) {
                if (BTreeLeafPage.this.bitMapHeaderArray[i]) {
                    // 过滤掉为recordList为空的插槽
                    noEmptyRecordArrayList.add(BTreeLeafPage.this.recordArray[i]);
                }
            }

            if(isReverse){
                Collections.reverse(noEmptyRecordArrayList);
            }

            iter = noEmptyRecordArrayList.iterator();
        }

        @Override
        public boolean hasNext() {
            return this.iter.hasNext();
        }

        @Override
        public Record next() {
            return this.iter.next();
        }
    }
}
