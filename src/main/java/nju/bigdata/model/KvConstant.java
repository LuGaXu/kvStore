package nju.bigdata.model;

/**
 * Created by lujxu on 2017/10/26.
 */
public class KvConstant {
   // public static final String PERSIST_FILE_PATH="/opt/localdisk";
    //test
    public static final String PERSIST_FILE_PATH="E:";
    /**
     * 一个log存满16M即为存满
     */
    public static final double FULL_FILE_SIZE=1;
    /**
     * 一个log存储的k-v对的最大值
     */
    public static final int MAX_OBJECT_COUNT=100000;
   /**
   * 换行符
   */
   public static final  String LINE_SEPERATOR=System.getProperty("line.separator");
}
