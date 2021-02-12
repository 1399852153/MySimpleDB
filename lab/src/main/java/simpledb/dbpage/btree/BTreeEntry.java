package simpledb.dbpage.btree;

import simpledb.dbrecord.RecordId;
import simpledb.matadata.fields.Field;

/**
 * @author xiongyx
 * @date 2021/2/12
 */
public class BTreeEntry {

    /**
     * The key of this entry
     * */
    private Field key;

    /**
     * The left child page id
     * */
    private BTreePageId leftChild;

    /**
     * The right child page id
     * */
    private BTreePageId rightChild;

    /**
     * The record id of this entry
     * */
    private RecordId recordId; // null if not stored on any page

    public BTreeEntry(Field key, BTreePageId leftChild, BTreePageId rightChild) {
        this.key = key;
        this.leftChild = leftChild;
        this.rightChild = rightChild;
    }

    public Field getKey() {
        return key;
    }

    public void setKey(Field key) {
        this.key = key;
    }

    public BTreePageId getLeftChild() {
        return leftChild;
    }

    public void setLeftChild(BTreePageId leftChild) {
        this.leftChild = leftChild;
    }

    public BTreePageId getRightChild() {
        return rightChild;
    }

    public void setRightChild(BTreePageId rightChild) {
        this.rightChild = rightChild;
    }

    public RecordId getRecordId() {
        return recordId;
    }

    public void setRecordId(RecordId recordId) {
        this.recordId = recordId;
    }

    /**
     * Prints a representation of this BTreeEntry
     */
    public String toString() {
        return "[" +
                (leftChild != null ? String.valueOf(leftChild.getPageNo()) : "null") +
                "|" + key + "|" +
                (rightChild != null ? String.valueOf(rightChild.getPageNo()) : "null") +
                "]";
    }
}
