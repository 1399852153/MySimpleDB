package simpledb.matadata.fields;

import simpledb.matadata.types.ColumnTypeEnum;

import java.io.DataOutputStream;

/**
 * @author xiongyx
 * @date 2021/2/1
 */
public interface Field {

    void serialize(DataOutputStream dos);

    // boolean compare(Predicate predicate, Field value);

    ColumnTypeEnum getType();
}
