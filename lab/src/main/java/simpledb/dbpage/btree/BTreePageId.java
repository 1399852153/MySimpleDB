package simpledb.dbpage.btree;

import simpledb.dbpage.PageId;

/**
 * @author xiongyx
 * @date 2021/2/11
 */
public class BTreePageId implements PageId {

    private final String tableId;
    private final int pageNo;

    public BTreePageId(String tableId, int pageNo) {
        this.tableId = tableId;
        this.pageNo = pageNo;
    }

    @Override
    public String getTableId() {
        return tableId;
    }

    @Override
    public int getPageNo() {
        return pageNo;
    }

    @Override
    public String toString() {
        return "BTreePageId{" +
                "tableId=" + tableId +
                ", pageNo=" + pageNo +
                '}';
    }
}
