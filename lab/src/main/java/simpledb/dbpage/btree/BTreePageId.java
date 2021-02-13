package simpledb.dbpage.btree;

import simpledb.dbpage.PageId;

/**
 * @author xiongyx
 * @date 2021/2/11
 */
public class BTreePageId implements PageId {

    private final String tableId;
    private final int pageNo;

    /**
     * 当前页对应的，b树索引类型
     * @see BTreePageCategoryEnum
     * */
    private int pageCategory;

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

    public int getPageCategory() {
        return pageCategory;
    }

    @Override
    public String toString() {
        return "BTreePageId{" +
                "tableId=" + tableId +
                ", pageNo=" + pageNo +
                '}';
    }
}
