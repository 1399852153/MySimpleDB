package simpledb.dbpage.btree;

import simpledb.Database;
import simpledb.dbpage.DBPage;
import simpledb.dbpage.PageCommonUtil;
import simpledb.dbpage.PageId;
import simpledb.exception.DBException;
import simpledb.exception.ParseException;
import simpledb.matadata.fields.IntField;
import simpledb.matadata.types.ColumnTypeEnum;
import simpledb.util.CommonUtil;

import java.io.*;
import java.util.Arrays;
import java.util.Iterator;

/**
 * @author xiongyx
 * @date 2021/2/13
 */
public class BTreeHeaderPage implements DBPage {

    private final BTreePageId bTreePageId;
    private boolean[] bitMapHeaderArray;
    private final int maxSlotNum;

    private int nextPage; // next header page or 0
    private int prevPage; // previous header page or 0

    public BTreeHeaderPage(BTreePageId bTreePageId, byte[] data) {
        this.bTreePageId = bTreePageId;
        this.maxSlotNum = getMaxSlotNum();

        try {
            deSerialize(data);
        } catch (IOException e) {
            throw new ParseException("deSerialize BTreeHeaderPage error", e);
        }
    }

    private void deSerialize(byte[] data) throws IOException{
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // 读取nextPage、prevPage
        IntField nextF = (IntField) ColumnTypeEnum.INT_TYPE.parse(dis);
        this.nextPage = nextF.getValue();
        IntField prevF = (IntField) ColumnTypeEnum.INT_TYPE.parse(dis);
        this.prevPage = prevF.getValue();

        // 读取header位图
        this.bitMapHeaderArray = new boolean[getHeaderSize()];
        for (int i=0; i<bitMapHeaderArray.length; i++) {
            bitMapHeaderArray[i] = dis.readBoolean();
        }
        // 不满一个字节的，将其跳过
        int needSkip = CommonUtil.bitCeilByte(this.maxSlotNum);
        for (int i = 0; i < needSkip; i++) {
            dis.readBoolean();
        }

        dis.close();
    }

    @Override
    public byte[] serialize() throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(Database.getBufferPool().getPageSize());
        DataOutputStream dos = new DataOutputStream(byteArrayOutputStream);

        dos.writeInt(nextPage);
        dos.writeInt(prevPage);
        // 写入头部位图
        for (boolean b : this.bitMapHeaderArray) {
            dos.writeBoolean(b);
        }
        // 不满一个字节的，将其跳过
        int needSkip = CommonUtil.bitCeilByte(this.maxSlotNum);
        for (int i = 0; i < needSkip; i++) {
            dos.writeBoolean(false);
        }

        dos.flush();

        return byteArrayOutputStream.toByteArray();
    }

    @Override
    public int getNotEmptySlotsNum() {
        return PageCommonUtil.getNotEmptySlotsNum(this.bitMapHeaderArray);
    }

    @Override
    public int getMaxSlotNum() {
        return getHeaderSize();
    }

    /**
     * get the index of the first empty slot
     * @return the index of the first empty slot or -1 if none exists
     */
    public int getFirstEmptySlotIndex() {
        return PageCommonUtil.getFirstEmptySlotIndex(this.bitMapHeaderArray,false);
    }

    @Override
    public BTreePageId getPageId() {
        return this.bTreePageId;
    }

    @Override
    public Iterator iterator() {
        throw new DBException("un support");
    }

    @Override
    public Iterator reverseIterator() {
        throw new DBException("un support");
    }

    /**
     * Get the page id of the previous header page
     * @return the page id of the previous header page
     */
    public BTreePageId getPrevPageId() {
        if(prevPage == 0) {
            return null;
        }
        return new BTreePageId(this.bTreePageId.getTableId(), prevPage, BTreePageCategoryEnum.HEADER.getValue());
    }

    /**
     * Get the page id of the next header page
     * @return the page id of the next header page
     */
    public BTreePageId getNextPageId() {
        if(nextPage == 0) {
            return null;
        }
        return new BTreePageId(this.bTreePageId.getTableId(), nextPage, BTreePageCategoryEnum.HEADER.getValue());
    }

    public void markSlotUsed(int index){
        this.bitMapHeaderArray[index] = true;
    }

    public void markSlotNotUsed(int index){
        this.bitMapHeaderArray[index] = false;
    }

    /**
     * Set the page id of the previous header page
     * @param id - the page id of the previous header page
     */
    public void setPrevPageId(BTreePageId id) {
        if(id == null) {
            prevPage = 0;
        }
        else {
            if(!id.getTableId().equals(this.bTreePageId.getTableId())) {
                throw new DBException("table id mismatch in setPrevPageId");
            }
            if(id.getPageCategory() !=  BTreePageCategoryEnum.HEADER.getValue()) {
                throw new DBException("prevPage must be a header page");
            }
            prevPage = id.getPageNo();
        }
    }

    /**
     * Set the page id of the next header page
     * @param id - the page id of the next header page
     */
    public void setNextPageId(BTreePageId id) {
        if(id == null) {
            nextPage = 0;
        }
        else {
            if(!id.getTableId().equals(this.bTreePageId.getTableId())) {
                throw new DBException("table id mismatch in setNextPageId");
            }
            if(id.getPageCategory() != BTreePageCategoryEnum.HEADER.getValue()) {
                throw new DBException("nextPage must be a header page");
            }
            nextPage = id.getPageNo();
        }
    }

    /**
     * Initially mark all slots in the header used.
     */
    public void init() {
        // 整个header的bit位全部用1填充
        Arrays.fill(this.bitMapHeaderArray, true);
    }

    public static int getHeaderSize(){
        int headerSpaceByte = Database.getBufferPool().getPageSize() - BTreeConstants.INDEX_SIZE * 2;

        return headerSpaceByte;
    }
}
