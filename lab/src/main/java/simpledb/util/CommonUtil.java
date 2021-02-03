package simpledb.util;

/**
 * @author xiongyx
 * @date 2021/2/3
 */
public class CommonUtil {

    public static int bitCeilByte(int bitNum){
        return (8-bitNum%8)%8;
    }

    public static void main(String[] args) {
        System.out.println(bitCeilByte(29));
    }
}
