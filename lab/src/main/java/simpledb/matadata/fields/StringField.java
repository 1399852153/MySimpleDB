package simpledb.matadata.fields;

import simpledb.exception.ParseException;
import simpledb.matadata.types.ColumnTypeEnum;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author xiongyx
 * @date 2021/2/1
 */
public class StringField implements Field {

    /**
     * 目前的设计里和simpleDB一样，string字段无论实际长度，在磁盘中都占用MAX_LENGTH大小的空间
     * */
    public static final int MAX_LENGTH = 128;

    private String value;
    private final ColumnTypeEnum columnTypeEnum = ColumnTypeEnum.STRING_TYPE;

    public StringField(String value){
        assert value.length() <= MAX_LENGTH;
        this.value = value;
    }

    public StringField(DataInputStream dis) {
        String businessData;
        try {
            // 读取出string字段的实际长度
            int realSize = dis.readInt();
            // 读取出实际的业务数据
            byte[] bs = new byte[realSize];
            dis.read(bs);
            businessData = new String(bs);
            // 磁盘中存储的string字段长度是定长的，需要跳过对应的长度
            dis.skipBytes(MAX_LENGTH - realSize);
        } catch (IOException e) {
            throw new ParseException("StringField init error");
        }

        // 超过MAX_LENGTH需要截断
        if (businessData.length() > MAX_LENGTH){
            this.value = businessData.substring(0, MAX_LENGTH);
        } else{
            this.value = businessData;
        }
    }

    @Override
    public void serialize(DataOutputStream dos) {
        int overflow = MAX_LENGTH - this.value.length();
        if (overflow < 0) {
            value = value.substring(0, MAX_LENGTH);
        }
        try {
            dos.writeInt(value.length());
            dos.writeBytes(value);

            while (overflow-- > 0){
                // 磁盘中存储的string字段长度是定长的，剩余的空间用0填充
                dos.write((byte) 0);
            }
        } catch (IOException e) {
            throw new ParseException("StringField serialize error");
        }
    }

    @Override
    public ColumnTypeEnum getType() {
        return columnTypeEnum;
    }
}
