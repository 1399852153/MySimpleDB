package simpledb.matadata.table;

import simpledb.matadata.types.ColumnType;
import simpledb.matadata.types.ColumnTypeEnum;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * @author xiongyx
 * @date 2021/2/1
 */
public class TableDesc {

    private String tableId;
    private final List<TableDescItem> tableDescItemList;

    public TableDesc(ColumnTypeEnum[] columnTypeEnumList){
        this.tableId = UUID.randomUUID().toString();
        this.tableDescItemList = new ArrayList<>();

        for(int i=0; i<columnTypeEnumList.length; i++){
            ColumnTypeEnum item = columnTypeEnumList[i];
            tableDescItemList.add(new TableDescItem("f"+i,item));
        }
    }

    public TableDesc(String tableId, ColumnTypeEnum[] columnTypeEnumList) {
        this(columnTypeEnumList);
        this.tableId = tableId;
    }

    public TableDesc(List<TableDescItem> tableDescItemList) {
        this(UUID.randomUUID().toString(),tableDescItemList);
    }

    public TableDesc(String tableId, List<TableDescItem> tableDescItemList) {
        this.tableId = tableId;
        this.tableDescItemList = tableDescItemList;
    }

    public String getTableId() {
        return tableId;
    }

    public List<ColumnTypeEnum> getColumnTypeEnumList() {
        return tableDescItemList.stream().map(TableDescItem::getColumnTypeEnum).collect(Collectors.toList());
    }

    public int getSize(){
        return tableDescItemList.stream()
                .map(TableDescItem::getColumnTypeEnum)
                .mapToInt(ColumnType::getLength)
                .sum();
    }

    public int getColumnNum(){
        return tableDescItemList.size();
    }

    public TableDescItem getColumn(int index){
        return tableDescItemList.get(index);
    }
}
