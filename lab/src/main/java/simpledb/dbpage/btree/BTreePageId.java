package simpledb.dbpage.btree;

import simpledb.dbpage.PageId;

import java.util.Objects;

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

    public BTreePageId(String tableId, int pageNo, int pageCategory) {
        this.tableId = tableId;
        this.pageNo = pageNo;
        this.pageCategory = pageCategory;
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BTreePageId that = (BTreePageId) o;
        return pageNo == that.pageNo && pageCategory == that.pageCategory && Objects.equals(tableId, that.tableId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, pageNo, pageCategory);
    }

    @Override
    public String toString() {
        return "BTreePageId{" +
                "tableId=" + tableId +
                ", pageNo=" + pageNo +
                '}';
    }
}
