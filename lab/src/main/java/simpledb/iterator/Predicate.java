package simpledb.iterator;

import simpledb.dbrecord.Record;
import simpledb.iterator.enums.OperatorEnum;
import simpledb.matadata.fields.Field;

/**
 * @author xiongyx
 * @date 2021/2/1
 */
public class Predicate {

    private OperatorEnum operatorEnum;
    private Field compareTarget;
    private int targetColumnIndex;

    public boolean filter(Record record){
        Field recordField = record.getField(targetColumnIndex);
        return recordField.compare(operatorEnum,compareTarget);
    }
}
