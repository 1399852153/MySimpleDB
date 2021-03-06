package simpledb.dbpage.normal;

import simpledb.Database;
import simpledb.dbpage.DBPage;
import simpledb.dbpage.PageCommonUtil;
import simpledb.dbrecord.Record;
import simpledb.dbrecord.RecordId;
import simpledb.exception.DBException;
import simpledb.exception.ParseException;
import simpledb.matadata.fields.Field;
import simpledb.matadata.table.TableDesc;
import simpledb.matadata.types.ColumnTypeEnum;
import simpledb.util.CommonUtil;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author xiongyx
 * @date 2021/2/2
 */
public class DBHeapPage implements DBPage {

    private final TableDesc tableDesc;
    private final HeapPageId pageId;
    private final int maxSlotNum;

    /**
     * header位图 true表示存在，false表示不存在 (注意：目前的设计里一个boolean在磁盘中占1字节)
     * */
    private boolean[] bitMapHeaderArray;
    private Record[] recordArray;

    public DBHeapPage(TableDesc tableDesc, HeapPageId pageId, byte[] data) {
        try {
            this.tableDesc = tableDesc;
            this.pageId = pageId;
            this.maxSlotNum = getMaxSlotNum();
            deSerialize(data);
        } catch (IOException e) {
            throw new ParseException("deSerialize DBHeapPage error",e);
        }
    }

    /**
     * 反序列化 磁盘二进制数据->内存结构化数据
     * */
    private void deSerialize(byte[] data) throws IOException {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // 读取文件流，构造header位图
        this.bitMapHeaderArray = new boolean[this.getMaxSlotNum()];
        for (int i=0; i<bitMapHeaderArray.length; i++) {
            this.bitMapHeaderArray[i] = dis.readBoolean();
        }

        // 读取文件流，根据读取出相应的数据记录
        this.recordArray = new Record[this.maxSlotNum];
        for (int i=0; i<this.recordArray.length; i++){
            this.recordArray[i] = readNextRecord(dis,i);
        }

        dis.close();
    }

    /**
     * 序列化 内存结构化数据->磁盘二进制数据
     * */
    @Override
    public byte[] serialize() throws IOException {
        int len = Database.getBufferPool().getPageSize();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(byteArrayOutputStream);

        // 写入位图
        for (boolean b : this.bitMapHeaderArray) {
            dos.writeBoolean(b);
        }

        // 写入record
        for (int i=0; i<this.recordArray.length; i++) {
            writeNextRecord(dos,i);
        }

        // 如果实际不足一页，用0填充页内剩余的空间(实际数据无法和页大小恰好对齐)
        // 字节为单位 (页大小 - （位图大小 + record长度 * record数量）)
        int needPaddingLength = Database.getBufferPool().getPageSize() - (this.bitMapHeaderArray.length + this.tableDesc.getSize() * this.recordArray.length); //- numSlots * td.getSize();
        if(needPaddingLength > 0){
            byte[] zeroes = new byte[needPaddingLength];
            // 后续空余的空间，用0补齐
            dos.write(zeroes, 0, needPaddingLength);
        }

        // 强制刷新
        dos.flush();

        return byteArrayOutputStream.toByteArray();
    }

    private Record readNextRecord(DataInputStream dis, int slotIndex) {
        if (!this.bitMapHeaderArray[slotIndex]) {
            // 位图显示记录不存在，文件指针直接跳过一个record的长度
            for (int i=0; i<this.tableDesc.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty record");
                }
            }
            return null;
        }else{
            // 位图显示记录存在
            Record record = new Record();
            RecordId recordId = new RecordId(this.pageId,slotIndex);
            record.setRecordId(recordId);
            record.setTableDesc(this.tableDesc);

            List<ColumnTypeEnum> columnTypeEnumList = this.tableDesc.getColumnTypeEnumList();
            // 按照表结构定义，读取出每一个字段的数据
            List<Field> fieldList = columnTypeEnumList.stream()
                    .map(item-> item.parse(dis))
                    .collect(Collectors.toList());

            record.setFieldList(fieldList);

            return record;
        }
    }

    private void writeNextRecord(DataOutputStream dos,int slotIndex) throws IOException {
        if (!this.bitMapHeaderArray[slotIndex]) {
            // 空的插槽，用0填充一个record大小的空间
            for (int j=0; j<this.tableDesc.getSize(); j++) {
                dos.writeByte(0);
            }
        }else{
            // 非空插槽，写入数据

            Record recordItem = this.recordArray[slotIndex];
            // 按照表结构定义，向dos流按照顺序写出每一个字段的数据
            recordItem.getFieldList().forEach(
                    item->item.serialize(dos)
            );
        }
    }

    // ==============================接口方法===========================

    /**
     * 插入一条记录
     * */
    public void insertRecord(Record newRecord){
        if (!newRecord.getTableDesc().equals(tableDesc)){
            throw new DBException("table desc not match");
        }

        int emptySlotIndex = PageCommonUtil.getFirstEmptySlotIndex(this.bitMapHeaderArray,true);
        this.recordArray[emptySlotIndex] = newRecord;
        this.bitMapHeaderArray[emptySlotIndex] = true;
    }

    /**
     * 删除一条记录
     * */
    public void deleteRecord(Record recordNeedDelete){
        if (!recordNeedDelete.getTableDesc().equals(tableDesc)){
            throw new DBException("table desc not match");
        }
        if(!recordNeedDelete.getRecordId().getPageId().equals(this.pageId)){
            throw new DBException("pageId not match");
        }

        for(int i=0; i<recordArray.length; i++){
            Record recordItem = recordArray[i];
            if(bitMapHeaderArray[i] && recordItem.getRecordId().equals(recordNeedDelete.getRecordId())){
                bitMapHeaderArray[i] = false;
                recordArray[i] = null;
            }
        }
    }

    @Override
    public int getNotEmptySlotsNum() {
        return PageCommonUtil.getNotEmptySlotsNum(this.bitMapHeaderArray);
    }

    @Override
    public int getMaxSlotNum(){
        // （每一个tuple的字节数+header 1字节）
        int perRecordBit = tableDesc.getSize() + 1;

        // return返回值：HeapPage一页能容纳的tuple最大数量，向下取整（slot插槽数）
        return Database.getBufferPool().getPageSize() / perRecordBit;
    }

    @Override
    public HeapPageId getPageId() {
        return pageId;
    }

    @Override
    public Iterator<Record> iterator() {
        return new HeapPageIterator(false);
    }

    @Override
    public Iterator<Record> reverseIterator() {
        return new HeapPageIterator(true);
    }

    // =============================== Page页迭代器 ====================================

    private class HeapPageIterator implements Iterator<Record> {
        private final Iterator<Record> iter;

        public HeapPageIterator(boolean isReverse) {
            ArrayList<Record> noEmptyRecordArrayList = new ArrayList<>();
            for (int i = 0; i < DBHeapPage.this.maxSlotNum; i++) {
                if (DBHeapPage.this.bitMapHeaderArray[i]) {
                    // 过滤掉为recordList为空的插槽
                    noEmptyRecordArrayList.add(DBHeapPage.this.recordArray[i]);
                }
            }

            if(isReverse){
                Collections.reverse(noEmptyRecordArrayList);
            }

            iter = noEmptyRecordArrayList.iterator();
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public Record next() {
            return iter.next();
        }
    }
}
