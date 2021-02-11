package simpledb.dbpage.normal;

import simpledb.dbpage.PageId;

import java.util.Objects;

/**
 * @author xiongyx
 * @date 2021/2/2
 */
public class HeapPageId implements PageId {

    /**
     * 表示当前页是属于那张表的
     * */
    private final String tableId;

    /**
     * 对应表文件的第几页
     * */
    private final int pageNo;

    public HeapPageId(String tableId, int pageNo) {
        this.tableId = tableId;
        this.pageNo = pageNo;
    }

    public String getTableId() {
        return tableId;
    }

    public int getPageNo() {
        return pageNo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HeapPageId pageId = (HeapPageId) o;
        return pageNo == pageId.pageNo && Objects.equals(tableId, pageId.tableId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, pageNo);
    }

    @Override
    public String toString() {
        return "PageId{" +
                "tableId='" + tableId + '\'' +
                ", pageNo=" + pageNo +
                '}';
    }
}
