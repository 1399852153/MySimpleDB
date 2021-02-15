package simpledb.dbpage.btree;

import simpledb.dbpage.DBPage;
import simpledb.dbpage.PageId;
import simpledb.dbrecord.Record;
import simpledb.exception.DBException;
import simpledb.exception.ParseException;

import java.io.*;
import java.util.Iterator;

/**
 * @author xiongyx
 * @date 2021/2/13
 */
public class BTreeRootPtrPage implements DBPage {

    // size of this page
    public final static int ROOT_PTR_PAGE_SIZE = 4+1+4;

    private final BTreePageId bTreePageId;

    private int root;
    private int rootCategory;
    private int header;

    public BTreeRootPtrPage(BTreePageId bTreePageId, byte[] data) {
        this.bTreePageId = bTreePageId;

        try {
            deSerialize(data);
        } catch (IOException e) {
            throw new ParseException("deSerialize BTreeRootPtrPage error", e);
        }
    }

    private void deSerialize(byte[] data) throws IOException{
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        root = dis.readInt();
        rootCategory = dis.readByte();
        header = dis.readInt();
    }

    @Override
    public byte[] serialize() throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(ROOT_PTR_PAGE_SIZE);
        DataOutputStream dos = new DataOutputStream(byteArrayOutputStream);

        dos.writeInt(root);
        dos.writeByte((byte) rootCategory);
        dos.writeInt(header);

        dos.flush();

        return byteArrayOutputStream.toByteArray();
    }

    @Override
    public int getNotEmptySlotsNum() {
        throw new DBException("un support");
    }

    @Override
    public int getMaxSlotNum() {
        throw new DBException("un support");
    }

    @Override
    public PageId getPageId() {
        return this.bTreePageId;
    }

    @Override
    public Iterator<Record> iterator() {
        throw new DBException("un support");
    }

    @Override
    public Iterator<Record> reverseIterator() {
        throw new DBException("un support");
    }

    /**
     * Get the id of the root page in this B+ tree
     * @return the id of the root page
     */
    public BTreePageId getRootId() {
        if(root == 0) {
            return null;
        }
        return new BTreePageId(bTreePageId.getTableId(), root, rootCategory);
    }

    /**
     * Set the page id of the first header page
     * @param id - the id of the first header page
     */
    public void setHeaderId(BTreePageId id) {
        if(id == null) {
            header = 0;
        }
        else {
            if(!id.getTableId().equals(this.bTreePageId.getTableId())) {
                throw new DBException("table id mismatch in setHeaderId");
            }
            if(id.getPageCategory() != BTreePageCategoryEnum.HEADER.getValue()) {
                throw new DBException("header must be of type BTreePageId.HEADER");
            }
            header = id.getPageNo();
        }
    }

    public static byte[] createEmptyPageData() {
        return new byte[ROOT_PTR_PAGE_SIZE]; //all 0
    }

    public static BTreePageId getId(String tableid) {
        return new BTreePageId(tableid, 0, BTreePageCategoryEnum.ROOT_PTR.getValue());
    }
}
