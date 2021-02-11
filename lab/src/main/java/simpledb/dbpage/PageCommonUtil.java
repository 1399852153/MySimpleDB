package simpledb.dbpage;

import simpledb.exception.DBException;

/**
 * @author xiongyx
 * @date 2021/2/11
 */
public class PageCommonUtil {

    public static int getFirstEmptySlotIndex(boolean[] bitMapHeaderArray){
        for(int i=0; i<bitMapHeaderArray.length; i++){
            if(!bitMapHeaderArray[i]){
                return i;
            }
        }

        throw new DBException("can't find a empty slot");
    }
}
