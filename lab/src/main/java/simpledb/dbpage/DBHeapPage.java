package simpledb.dbpage;

import simpledb.BufferPool;
import simpledb.dbrecord.Record;
import simpledb.dbrecord.RecordId;
import simpledb.exception.DBException;
import simpledb.exception.ParseException;
import simpledb.matadata.fields.Field;
import simpledb.matadata.table.TableDesc;
import simpledb.matadata.types.ColumnTypeEnum;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

/**
 * @author xiongyx
 * @date 2021/2/2
 */
public class DBHeapPage implements Serializable {

    private TableDesc tableDesc;
    private PageId pageId;
    private int maxSlotNum;

    /**
     * header位图 true表示存在，false表示不存在
     * */
    private boolean[] bitMapHeaderArray;
    private Record[] recordArray;

    public DBHeapPage(TableDesc tableDesc,PageId id, byte[] data) {
        try {
            deSerialize(tableDesc,id,data);
        } catch (IOException e) {
            throw new ParseException("deSerialize DBHeapPage error",e);
        }
    }

    /**
     * 反序列化 磁盘二进制数据->内存结构化数据
     * */
    private void deSerialize(TableDesc tableDesc, PageId pageId, byte[] data) throws IOException {
        this.tableDesc = tableDesc;

        this.pageId = pageId;
        this.maxSlotNum = getMaxSlotNum();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // 读取文件流，构造header位图
        this.bitMapHeaderArray = new boolean[getHeaderSize()];
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
    private byte[] serialize() throws IOException {
        int len = BufferPool.getPageSize();
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
        // 字节为单位 (页大小 - （位图大小bit * 8 + record长度 * record数量）)
        int needPaddingLength = BufferPool.getPageSize() - (this.bitMapHeaderArray.length * 8 + this.tableDesc.getSize() * this.recordArray.length); //- numSlots * td.getSize();
        byte[] zeroes = new byte[needPaddingLength];
        // 后续空余的空间，用0补齐
        dos.write(zeroes, 0, needPaddingLength);

        // 强制刷新
        dos.flush();

        return byteArrayOutputStream.toByteArray();
    }

    private int getMaxSlotNum(){
        // BufferPool.getPageSize() * 8 => 每个HeapPage的字节数 * 8 => 每个HeapPage的字节数bit数（1Byte字节=8bit）
        // td.getSize() * 8 + 1 => 每一个tuple的字节数 * 8 + 1(每个tuple占HeapPage header位图的1bit位)
        // return返回值：HeapPage一页能容纳的tuple最大数量（slot插槽数）
        return (BufferPool.getPageSize() * 8) / (tableDesc.getSize() * 8 + 1);
    }

    private int getHeaderSize() {
        // page对应的slot插槽数决定了需要为header位图分配的内存大小
        // header位图是以Byte为单位分配的，因此需要分配的Byte数为插槽数/8，向上取整
        // 例如插槽数是9，那么必须要9/8=1.xxxx(向上取整)=2Byte共16bit的空间（1*8 < 9 < 2*8）来对应这9个插槽
        return (int)Math.ceil(getMaxSlotNum()*1.0/8);
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


    public boolean insertRecord(Record newRecord){
        if (!newRecord.getTableDesc().equals(tableDesc)){
            throw new DBException("table desc not match");
        }

        return true;
    }


    public boolean deleteRecord(Record recordNeedDelete){
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

        return true;
    }

    protected class HeapPageTupleIterator implements Iterator<Record> {
        private final Iterator<Record> iter;
        public HeapPageTupleIterator() {
            ArrayList<Record> tupleArrayList = new ArrayList<>();
            for (int i = 0; i < DBHeapPage.this.maxSlotNum; i++) {
                if (DBHeapPage.this.bitMapHeaderArray[i]) {
                    // 过滤掉为recordList为空的插槽
                    tupleArrayList.add(i, DBHeapPage.this.recordArray[i]);
                }
            }
            iter = tupleArrayList.iterator();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("TupleIterator: remove not supported");
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

    public Iterator<Record> iterator() {
        // some code goes here
        return new HeapPageTupleIterator();
    }
}
