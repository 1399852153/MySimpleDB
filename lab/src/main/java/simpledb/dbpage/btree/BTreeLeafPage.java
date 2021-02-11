package simpledb.dbpage.btree;

import simpledb.Database;
import simpledb.dbpage.DBPage;
import simpledb.dbpage.PageId;
import simpledb.dbrecord.Record;
import simpledb.exception.ParseException;
import simpledb.matadata.table.TableDesc;
import simpledb.matadata.types.ColumnTypeEnum;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author xiongyx
 * @date 2021/2/11
 *
 * b+树叶子结点
 */
public class BTreeLeafPage implements DBPage {

    public final static int INDEX_SIZE = ColumnTypeEnum.INT_TYPE.getLength();

    protected final BTreePageId pageId;
    protected final TableDesc tableDesc;
    protected int keyFieldIndex;

    protected int parent; // parent is always internal node or 0 for root node

    /**
     * 头部位图
     */
    private boolean[] header;

    /**
     * 所保存的数组列表
     * */
    private Record[] tuples;

    /**
     * 最大插槽数目
     * */
    private int maxSlotNum;

    /**
     * 左兄弟叶子节点页指针 0代表null
     * */
    private int leftSibling;

    /**
     * 有兄弟叶子节点页指针 0代表null
     * */
    private int rightSibling;

    public BTreeLeafPage(TableDesc tableDesc, BTreePageId pageId, byte[] data,int keyFieldIndex) {
        try {
            this.tableDesc = tableDesc;
            this.pageId = pageId;
            this.keyFieldIndex = keyFieldIndex;
            deSerialize(tableDesc,pageId,data);
        } catch (IOException e) {
            throw new ParseException("deSerialize BTreeLeafPage error",e);
        }
    }

    /**
     * 反序列化 磁盘二进制数据->内存结构化数据
     * */
    private void deSerialize(TableDesc tableDesc, BTreePageId pageId, byte[] data) throws IOException {
        this.maxSlotNum = this.getMaxSlotNum();

    }

    @Override
    public byte[] serialize() throws IOException {
        return new byte[0];
    }

    @Override
    public void insertRecord(Record newRecord) {

    }

    @Override
    public void deleteRecord(Record recordNeedDelete) {

    }

    @Override
    public int getNotEmptySlotsNum() {
        return 0;
    }

    @Override
    public int getMaxSlotNum() {
        // td.getSize() * 8 = 每一个tuple的bit位(Byte数 * 8) + 1位的header位图 => 每一个Tuple占用的空间
        int bitsPerTupleIncludingHeader = this.tableDesc.getSize() * 8 + 1;
        // extraBits are: left sibling pointer, right sibling pointer, parent pointer
        // 3 * INDEX_SIZE * 8 = 三个指针(左、右兄弟以及双亲节点指针)占据的bit数(Byte数 * 8)
        int extraBits = 3 * INDEX_SIZE * 8;
        // BTreeLeafPage页面可以容纳的最大插槽数 = 缓冲页面大小 - 额外的extraBits/每一个Tuple占用的空间
        int tuplesPerPage = (Database.getBufferPool().getPageSize()*8 - extraBits) / bitsPerTupleIncludingHeader; //round down
        return tuplesPerPage;
    }

    @Override
    public PageId getPageId() {
        return this.pageId;
    }

    @Override
    public Iterator<Record> iterator() {
        return null;
    }
}
