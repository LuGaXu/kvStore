package nju.bigdata.factory;

import com.sun.org.apache.regexp.internal.RE;
import nju.bigdata.model.KvConstant;

import java.util.*;

/**
 * Created by lujxu on 2017/10/22.
 */
public class KvMapFactory {

    private static  KvMapFactory kvMapFactory=new KvMapFactory();
    private Map<String,Map <String, String>> current;
    /**
     * 记录只读的map，对应于/opt/disk下的多个非当前log
     */
    private Queue<Map<String,Map <String, String>>> readOnlyQueue;

    private  KvMapFactory(){
        current= Collections.synchronizedSortedMap(new TreeMap<>());
        readOnlyQueue=new LinkedList<>();
    }

    public  static KvMapFactory getInstance(){
        return kvMapFactory;
    }

    public Map getCurrentKvMap(){
        return current;
    }

    public void put(String key,Map<String, String> value){
        current.put(key,value);
    }
    /**
     * 将当前map转为只读状态存入queue中，并新建current map
     * @return
     */
    public  boolean  changeCurrent(){
        boolean isInsert=this.readOnlyQueue.offer(current);
        if (isInsert)
            current=Collections.synchronizedSortedMap(new TreeMap<>());
        return isInsert;
    }

    /**
     * 获取即将被写入hdfs的map；若readOnlyQueue未空，返回null
     * @return
     */
    public synchronized Map<String,Map <String, String>> mapToBeWritten(){
        return this.readOnlyQueue.poll();
    }

    /**
     * 在readOnlyQueue中查找可以，若存在，返回true
     * @param key
     * @return
     */
    public boolean findKeyInQueue(String key){
        if(readOnlyQueue==null||readOnlyQueue.isEmpty())
            return false;
        boolean isExist=false;
        for(int i=0;i<readOnlyQueue.size();i++){
            Map<String,Map <String, String>> map=readOnlyQueue.poll();
            if(map.containsKey(key)){
                isExist=true;
            }
            readOnlyQueue.offer(map);
        }
        return  isExist;
    }

}
