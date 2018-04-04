package com.neusoft.tool;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author liujin
 *
 * @date 2018-3-29
 */
public class CommonUtil {

    /**  
     * 日期转换为字符串  
     * @param date  
     * @param pattern  
     * @return  
     */  
    public static String DateToString(Date date, String pattern) {  
      
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);  
        String string = simpleDateFormat.format(date);  
        return string;  
    } 
}
