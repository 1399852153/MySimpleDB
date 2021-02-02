package simpledb.matadata.types;

import simpledb.matadata.fields.Field;

import java.io.DataInputStream;

/**
 * @author xiongyx
 * @date 2021/2/1
 */
public interface ColumnType {

    int getLength();
    Field parse(DataInputStream dataInputStream);
}
