package simpledb.iterator;

import simpledb.dbrecord.Record;
import simpledb.iterator.enums.OperatorEnum;

/**
 * @author xiongyx
 * @date 2021/2/9
 */
public class JoinPredicate {

    private final int field1Index;
    private final int field2Index;
    private final OperatorEnum operatorEnum;

    public JoinPredicate(int field1Index, int field2Index, OperatorEnum operatorEnum) {
        this.field1Index = field1Index;
        this.field2Index = field2Index;
        this.operatorEnum = operatorEnum;
    }

    /**
     * join时的条件
     * */
    public boolean filter(Record record1, Record record2) {
        return record1.getField(field1Index).compare(operatorEnum, record2.getField(field2Index));
    }

    public int getField1Index() {
        return field1Index;
    }

    public int getField2Index() {
        return field2Index;
    }

    public OperatorEnum getOperatorEnum() {
        return operatorEnum;
    }
}
