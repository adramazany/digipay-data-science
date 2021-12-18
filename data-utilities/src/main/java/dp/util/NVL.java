package dp.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NVL {
    static Logger logger = LoggerFactory.getLogger(NVL.class);

    public static int getInt(Object src){
        return getInt(src,0);
    }
    public static int getInt(Object src,int defaultValue){
        if(src!=null){
            try{
                defaultValue = Integer.parseInt( src.toString() );
            }catch(Exception ex){
                logger.error(ex.getMessage());
            }
        }
        return defaultValue;
    }

    public static String getString(Object obj) {
        return getString(obj,"");
    }

    public static String getString(Object obj,String defaultValue) {
        if(obj!=null){
            try{
                defaultValue = obj.toString();
            }catch(Exception ex){
                logger.error(ex.getMessage());
            }
        }
        return defaultValue;
    }

}
