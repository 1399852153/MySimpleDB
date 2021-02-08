package simpledb.matadata.fields;

import simpledb.exception.DBException;
import simpledb.matadata.types.ColumnTypeEnum;

/**
 * @author xiongyx
 * @date 2021/2/8
 */
public class FieldFactory {

    public static Field getFieldByClass(ColumnTypeEnum columnTypeEnum,Object value){
        switch (columnTypeEnum){
            case INT_TYPE:
                return new IntField((Integer) value);
            case STRING_TYPE:
                return new StringField((String)value);
            default:
                throw new DBException("un support field type");

        }
    }
}
