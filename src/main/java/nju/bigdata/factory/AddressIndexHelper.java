package nju.bigdata.factory;

import cn.helium.kvstore.rpc.RpcServer;
import nju.bigdata.model.KvConstant;
import org.apache.commons.math3.analysis.function.Add;

import java.io.*;
import java.util.Observable;
import java.util.TreeMap;

/**
 * Created by lujxu on 2017/11/1.
 * 构造记录hdfs中文件地址的索引
 * id=5n时为merge第一次的文件地址，merge的文件id为5n-4 - 5n-1
 */
public class AddressIndexHelper extends Observable
{
    private TreeMap<Integer,String> addressIndex;
    public static  final String FILE_ADDRESS_MAP= KvConstant.PERSIST_FILE_PATH+"/fileAddressMap.txt";
    private String lineBreak=null;
    private static AddressIndexHelper addressIndexHelper=new AddressIndexHelper();

    private AddressIndexHelper(){
        lineBreak=System.getProperty( "line.separator" );
        addressIndex=new TreeMap<>();
        readMapFromFile();
        constructIndexInfo();
        MergeLogObserver observer=new MergeLogObserver(this);
    }

    public static AddressIndexHelper getInstance(){
        return addressIndexHelper;
    }

    /**
     * 返回key值所對應的文件名
     * key=0，返回索引文件名
     * @param key
     * @return
     */
    public String getAddress(int key){
        if(key<0)
            return null;
        int mergeKey=key+5-key%5;
        if(addressIndex.containsKey(mergeKey)&&key!=0)
            return addressIndex.get(mergeKey);
        return addressIndex.get(key);
    }

    /**
     * 获得新的key值
     * @return
     */
    public synchronized int createKey(){
        if(addressIndex.size()<=0)
            return -1;
        Integer lastKey=addressIndex.lastKey()+1;
        //需要merge
        if(lastKey%5==0){
            setChanged();
            notifyObservers(lastKey);
            lastKey++;
        }
        return lastKey;
    }

    /**
     * 在内存与文件中同时添加一条hdfs文件名的记录
     */
    public boolean addAddressIndex(int key, String address){
        File file=new File(FILE_ADDRESS_MAP);
        try {
            FileWriter writer=new FileWriter(file,true);
            StringBuffer buffer=new StringBuffer("");
            buffer.append(key);
            buffer.append(" ");
            buffer.append(address);
            buffer.append(lineBreak);
            writer.write(buffer.toString());
            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        addressIndex.put(key,address);
        return true;
    }

    /**
     * 从文件FILE_ADDRESS_MAP中读取addressIndex
     */
    private void readMapFromFile(){
        File addressFile=new File(FILE_ADDRESS_MAP);
        if(!addressFile.exists()){
            try {
                addressFile.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }finally {
                return;
            }
        }
        try {
            InputStreamReader reader=new InputStreamReader(new FileInputStream(addressFile));
            BufferedReader br=new BufferedReader(reader);
            try {
                String temp="";
                while ((temp=br.readLine())!=null){
                    String [] str=temp.split(" ");
                    if(str.length<2) continue;
                    Integer key=Integer.parseInt(str[0]);
                    addressIndex.put(key,str[1].trim());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * 构造索引文件的信息：key=0, value={hdfs中的文件名}
     */
    private void constructIndexInfo(){
        if(addressIndex.size()>0) return;
        String indexAddr="Index"+ RpcServer.getRpcServerId()+"_"+System.currentTimeMillis();
        addAddressIndex(0,indexAddr);
    }

    public static void  main(String []args){
        AddressIndexHelper h=new AddressIndexHelper();
        System.out.println(h.getAddress(2));

    }

}
