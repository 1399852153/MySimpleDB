package simpledb.iterator;

import simpledb.dbrecord.Record;
import simpledb.iterator.enums.OperatorEnum;
import simpledb.matadata.fields.Field;

/**
 * @author xiongyx
 * @date 2021/2/1
 */
public class Predicate {

    private final OperatorEnum operatorEnum;
    private final Field compareTarget;
    private final int targetColumnIndex;

    public Predicate(OperatorEnum operatorEnum, Field compareTarget, int targetColumnIndex) {
        this.operatorEnum = operatorEnum;
        this.compareTarget = compareTarget;
        this.targetColumnIndex = targetColumnIndex;
    }

    public boolean filter(Record record){
        Field recordField = record.getField(targetColumnIndex);
        return recordField.compare(operatorEnum,compareTarget);
    }
}
