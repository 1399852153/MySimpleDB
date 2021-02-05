package simpledb.matadata.fields;

import simpledb.exception.DBException;
import simpledb.exception.ParseException;
import simpledb.iterator.Predicate;
import simpledb.iterator.enums.OperatorEnum;
import simpledb.matadata.types.ColumnTypeEnum;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author xiongyx
 * @date 2021/2/1
 */
public class IntField implements Field {

    private int value;
    private final ColumnTypeEnum columnType = ColumnTypeEnum.INT_TYPE;

    public IntField(int value) {
        this.value = value;
    }

    public IntField(DataInputStream dos) {
        try {
            this.value = dos.readInt();
        } catch (IOException e) {
            throw new ParseException("IntField init error");
        }
    }

    @Override
    public void serialize(DataOutputStream dos) {
        try {
            dos.writeInt(value);
        } catch (IOException e) {
            throw new ParseException("IntField serialize error");
        }
    }

    @Override
    public boolean compare(OperatorEnum operatorEnum, Field value) {
        IntField iVal = (IntField) value;

        switch (operatorEnum) {
            case EQUALS:
            case LIKE:
                return this.value == iVal.value;

            case NOT_EQUALS:
                return this.value != iVal.value;

            case GREATER_THAN:
                return this.value > iVal.value;

            case GREATER_THAN_OR_EQ:
                return this.value >= iVal.value;

            case LESS_THAN:
                return this.value < iVal.value;

            case LESS_THAN_OR_EQ:
                return this.value <= iVal.value;

            default:
                throw new DBException("un support operatorEnum op=" + operatorEnum);
        }
    }

    @Override
    public ColumnTypeEnum getType() {
        return columnType;
    }

    @Override
    public String toString() {
        return "IntField{" +
                "value=" + value +
                '}';
    }
}
