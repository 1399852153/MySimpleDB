package simpledb.matadata.fields;

import simpledb.iterator.Predicate;
import simpledb.iterator.enums.OperatorEnum;
import simpledb.matadata.types.ColumnTypeEnum;

import java.io.DataOutputStream;

/**
 * @author xiongyx
 * @date 2021/2/1
 */
public interface Field {

    void serialize(DataOutputStream dos);

    boolean compare(OperatorEnum operatorEnum, Field value);

    ColumnTypeEnum getType();

    String toString();

    Object getValue();
}
