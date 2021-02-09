package simpledb.matadata.table;

import simpledb.matadata.types.ColumnTypeEnum;

/**
 * @author xiongyx
 * @date 2021/2/9
 */
public class TableDescItem {

    private String fieldName;
    private ColumnTypeEnum columnTypeEnum;

    public TableDescItem(String fieldName, ColumnTypeEnum columnTypeEnum) {
        this.fieldName = fieldName;
        this.columnTypeEnum = columnTypeEnum;
    }

    public String getFieldName() {
        return fieldName;
    }

    public ColumnTypeEnum getColumnTypeEnum() {
        return columnTypeEnum;
    }
}
