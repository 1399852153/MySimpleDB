package simpledb.matadata.table;

import simpledb.matadata.types.ColumnType;
import simpledb.matadata.types.ColumnTypeEnum;

import java.util.List;

/**
 * @author xiongyx
 * @date 2021/2/1
 */
public class TableDesc {

    private String tableId;
    private List<ColumnTypeEnum> columnTypeEnumList;

    public TableDesc(String tableId, List<ColumnTypeEnum> tableColumnList) {
        this.tableId = tableId;
        this.columnTypeEnumList = tableColumnList;
    }

    public String getTableId() {
        return tableId;
    }

    public List<ColumnTypeEnum> getColumnTypeEnumList() {
        return columnTypeEnumList;
    }

    public int getSize(){
        return columnTypeEnumList.stream()
                .mapToInt(ColumnType::getLength)
                .sum();
    }

    public int getColumnNum(){
        return columnTypeEnumList.size();
    }

    public ColumnTypeEnum getColumn(int index){
        return columnTypeEnumList.get(index);
    }
}
