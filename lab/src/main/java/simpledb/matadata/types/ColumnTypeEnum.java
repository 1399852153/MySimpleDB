package simpledb.matadata.types;

import simpledb.matadata.fields.Field;
import simpledb.matadata.fields.IntField;
import simpledb.matadata.fields.StringField;

import java.io.DataInputStream;

/**
 * @author xiongyx
 * @date 2021/2/1
 */
public enum ColumnTypeEnum implements ColumnType{
    INT_TYPE(){
        @Override
        public int getLength() {
            return 4;
        }

        @Override
        public Field parse(DataInputStream dataInputStream) {
            return new IntField(dataInputStream);
        }
    },
    STRING_TYPE(){
        @Override
        public int getLength() {
            return 4 + StringField.MAX_LENGTH;
        }

        @Override
        public Field parse(DataInputStream dataInputStream) {
            return new StringField(dataInputStream);
        }
    },
    ;


}
