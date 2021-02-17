package simpledb.dbpage;

import simpledb.Database;
import simpledb.dbpage.btree.BTreePage;
import simpledb.exception.DBException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author xiongyx
 * @date 2021/2/11
 */
public class PageCommonUtil {

    public static int getFirstEmptySlotIndex(boolean[] bitMapHeaderArray,boolean mustExist){
        for(int i=0; i<bitMapHeaderArray.length; i++){
            if(!bitMapHeaderArray[i]){
                return i;
            }
        }

        if(mustExist){
            throw new DBException("can't find a empty slot");
        }else{
            return -1;
        }
    }

    public static int getNotEmptySlotsNum(boolean[] bitMapHeaderArray){
        int notEmptySlotNum = 0;
        for (boolean b : bitMapHeaderArray) {
            if (b) {
                notEmptySlotNum += 1;
            }
        }
        return notEmptySlotNum;
    }

    public static byte[] createEmptyPageData() {
        return new byte[Database.getBufferPool().getPageSize()]; //all 0
    }

    /**
     * B+树数据页是否低于阈值(当前存储的记录数量低于最大容量的一半)
     * */
    public static int lowThreshold(BTreePage bTreePage){
        return bTreePage.getMaxSlotNum() - bTreePage.getMaxSlotNum()/2;
    }

//    /**
//     * 位图数组=>文件中的字节数组
//     * */
//    public static Byte[] bitMapToFileByteArray(Boolean[] bitMap){
//        List<Byte> byteList = new ArrayList<>();
//
//        int byteSize = 8;
//        List<Boolean> byteItem = new ArrayList<>(byteSize);
//        for(int i=0; i<bitMap.length; i++){
//            byteItem.add(bitMap[i]);
//            if((i+1)%8 == 0){
//                byteList.add(toFileByte(byteItem));
//                byteItem.clear();
//            }
//        }
//
//        if(!byteItem.isEmpty()){
//            byteList.add(toFileByte(byteItem));
//        }
//
//        return byteList.toArray(new Byte[0]);
//    }
//
//    /**
//     * 文件中的字节数组=>位图数组
//     * */
//    public static Boolean[] fileByteArrayToBitMap(Byte[] byteArray, int maxLength){
//        if(maxLength == 0){
//            return new Boolean[0];
//        }
//
//        List<Boolean> bitMapList = new ArrayList<>();
//
//        for(byte byteItem : byteArray){
//            bitMapList.addAll(Arrays.asList(getBooleanArray(byteItem)));
//            if(bitMapList.size() >= maxLength){
//                break;
//            }
//        }
//
//        return bitMapList.toArray(new Boolean[0]);
//    }
//
//    /**
//     * 将一个长度为8的boolean数组（每bit代表一个boolean值）转换为byte
//     */
//    public static byte toFileByte(List<Boolean> array) {
//        if(array == null || array.isEmpty()){
//            return 0;
//        }
//
//        while (array.size() < 8){
//            array.add(false);
//        }
//
//        byte b = 0;
//        for(int i=0;i<=7;i++) {
//            if(array.get(i)){
//                int nn=(1<<(7-i));
//                b += nn;
//            }
//        }
//        return b;
//    }
//
//
//    /**
//     * 将byte转换为一个长度为8的boolean数组（每bit代表一个boolean值）
//     */
//    private static Boolean[] getBooleanArray(byte b) {
//        Boolean[] array = new Boolean[8];
//        for (int i = 7; i >= 0; i--) { //对于byte的每bit进行判定
//            array[i] = (b & 1) == 1;   //判定byte的最后一位是否为1，若为1，则是true；否则是false
//            b = (byte) (b >> 1);       //将byte右移一位
//        }
//        return array;
//    }

}
