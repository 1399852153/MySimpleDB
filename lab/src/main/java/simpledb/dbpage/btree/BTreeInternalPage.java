package simpledb.dbpage.btree;

import simpledb.Database;
import simpledb.dbpage.DBPage;
import simpledb.dbpage.PageId;
import simpledb.dbrecord.Record;
import simpledb.exception.ParseException;
import simpledb.matadata.fields.Field;
import simpledb.matadata.fields.IntField;
import simpledb.matadata.table.TableDesc;
import simpledb.matadata.table.TableDescItem;
import simpledb.matadata.types.ColumnTypeEnum;
import simpledb.util.CommonUtil;

import java.io.*;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author xiongyx
 * @date 2021/2/12
 */
public class BTreeInternalPage implements DBPage {

    private final BTreePageId pageId;
    private final TableDesc tableDesc;
    private final int keyFieldIndex;

    private int parent; // parent is always internal node or 0 for root node


    private boolean bitMapHeaderArray[];
    private Field keys[];
    private int children[];
    private int maxSlotNum;
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
        // 逻辑视图：
        //  k1 k2 k3
        // c1 c2 c3 c4
        keys[0] = null;
        for(int i=1; i<keys.length; i++){
            keys[i] = readNextKey(dis,i);
        }

        // 解析所存储的children
        this.children = new int[this.maxSlotNum];
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
    public void insertRecord(Record newRecord) {

    }

    @Override
    public void deleteRecord(Record recordNeedDelete) {

    }

    @Override
    public int getNotEmptySlotsNum() {
        int notEmptySlotNum = 0;
        for (boolean b : this.bitMapHeaderArray) {
            if (b) {
                notEmptySlotNum += 1;
            }
        }
        return notEmptySlotNum;
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

    @Override
    public Iterator<Record> iterator() {
        return null;
    }

    @Override
    public Iterator<Record> reverseIterator() {
        return null;
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
    private int readNextChild(DataInputStream dis, int slotIndex){
        if (!this.bitMapHeaderArray[slotIndex]) {
            // 位图显示记录不存在，文件指针直接跳过一个child索引的长度
            for (int i = 0; i < BTreeConstants.INDEX_SIZE; i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty btree record");
                }
            }
            return -1;
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
}
