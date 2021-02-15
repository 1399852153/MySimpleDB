package simpledb.dbpage;

import simpledb.Database;
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
}
