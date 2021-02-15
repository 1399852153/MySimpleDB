package simpledb.dbpage;

import simpledb.Database;
import simpledb.dbpage.btree.BTreePage;
import simpledb.exception.DBException;

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
}
