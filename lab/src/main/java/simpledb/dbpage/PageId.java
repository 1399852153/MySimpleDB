package simpledb.dbpage;

import java.util.Objects;

/**
 * @author xiongyx
 * @date 2021/2/2
 */
public class PageId {

    /**
     * 表示当前页是属于那张表的
     * */
    private String tableId;

    /**
     * 对应表文件的第几页
     * */
    private int pageNo;

    public PageId(String tableId, int pageNo) {
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
        PageId pageId = (PageId) o;
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
