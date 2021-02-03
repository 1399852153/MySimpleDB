package simpledb.matadata.fields;

import simpledb.exception.ParseException;
import simpledb.matadata.types.ColumnType;
import simpledb.matadata.types.ColumnTypeEnum;
import simpledb.predicate.Predicate;

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
    public ColumnTypeEnum getType() {
        return columnType;
    }
}
