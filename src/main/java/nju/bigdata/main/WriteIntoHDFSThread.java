package nju.bigdata.main;

import cn.helium.kvstore.rpc.RpcServer;
import nju.bigdata.factory.AddressIndexHelper;
import nju.bigdata.factory.FileSystemFactory;
import nju.bigdata.factory.LogFileFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import java.io.*;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by lujxu on 2017/11/2.
 */
public class WriteIntoHDFSThread implements Runnable {

    private String logFile;
    private Map<String, Map<String, String>> list;

    public WriteIntoHDFSThread(String logFile, Map<String, Map<String, String>> list ) {
        this.logFile=logFile;
        this.list=list;
    }

    @Override
    public void run() {
        writeIntoHDFS();
    }

    private synchronized void writeIntoHDFS() {

        String filename = "" + RpcServer.getRpcServerId() + "_" + System.currentTimeMillis();
        Path mapFile = new Path(FileSystemFactory.instance().getServerPath() + "/" + filename);

        MapFile.Writer.Option option1 = MapFile.Writer.keyClass(BytesWritable.class);
        SequenceFile.Writer.Option option2 = MapFile.Writer.valueClass(MapWritable.class);
        try {
            MapFile.Writer writer = new MapFile.Writer(FileSystemFactory.instance().getFileSystem().getConf(), mapFile,
                    option1, option2);

            //遍历map
            Iterator iter = list.keySet().iterator();
            BytesWritable subKey = new BytesWritable();
            MapWritable subValue = new MapWritable();
            String k;
            Map<String, String> v = new TreeMap<String, String>();
            while (iter.hasNext()) {
                k = (String) iter.next();
                v = list.get(k);
                subKey.set(new BytesWritable(k.getBytes()));
                for (Map.Entry<String, String> entry : v.entrySet()) {

                    subValue.put(new Text(entry.getKey()), new Text(entry.getValue()));
                }
                writer.append(subKey, subValue);
                subValue.clear();
            }

            //关闭write流
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        //将索引存入HDFS
         boolean result=insertIndex(filename,list);

        //清空已写入HDFS的日志文件
        if (result)
            LogFileFactory.instance().deleteLogFile(logFile);
    }

    private boolean insertIndex(String logFileName,Map<String,Map <String, String>> list){
        //先把日志文件的键值存入本地文件
        AddressIndexHelper helper=AddressIndexHelper.getInstance();
        int logFileKey=helper.createKey();
        helper.addAddressIndex(logFileKey,logFileName);

        //索引写入临时的MapFile
        String tmpName = "" + RpcServer.getRpcServerId() + "_" + System.currentTimeMillis();
        Path tmpFile=new Path(FileSystemFactory.instance().getServerPath()+"/tmp_"+tmpName);
        MapFile.Writer.Option option1=MapFile.Writer.keyClass(BytesWritable.class);
        SequenceFile.Writer.Option option2=SequenceFile.Writer.valueClass(IntWritable.class);

        MapFile.Writer writer=null;
        try {
            writer= new MapFile.Writer(FileSystemFactory.instance().getFileSystem().getConf(),tmpFile,
                    option1,option2);

            //遍历map
            Iterator iter=list.keySet().iterator();
            BytesWritable subKey=new BytesWritable();
            IntWritable subValue=new IntWritable();
            subValue.set(logFileKey);

            String k;
            while(iter.hasNext()){
                k=(String)iter.next();
                subKey.set(new BytesWritable(k.getBytes()));

                writer.append(subKey,subValue);

            }

            //关闭write流
            writer.close();

        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        //将临时文件与原索引文件进行merge
        String indexFileName=helper.getAddress(0);//索引文件的位置
        Path indexFile=new Path(FileSystemFactory.instance().getServerPath()+"/"+indexFileName);

        Path[] inMapFiles=new Path[2];
        inMapFiles[0]=indexFile;
        inMapFiles[1]=tmpFile;
        Path outMapFile=new Path(FileSystemFactory.instance().getServerPath()+"/"+indexFileName);

        return  mergeIndex(tmpName,indexFileName,inMapFiles,outMapFile);

    }

    private  boolean mergeIndex(String tmpName,String indexFileName,Path[] inMapFiles,Path outMapFile){
        try {
            //如果索引文件不存在
            if(!FileSystemFactory.instance().getFileSystem().exists(outMapFile)){
                MapFile.rename(FileSystemFactory.instance().getFileSystem(),
                        FileSystemFactory.instance().getServerPath()+"/tmp_"+tmpName,
                        FileSystemFactory.instance().getServerPath()+"/"+indexFileName);

            }else{

                MapFile.Merger merger=new MapFile.Merger(FileSystemFactory.instance().getFileSystem().getConf());
                merger.merge(inMapFiles,false,outMapFile);

                //删除临时文件
                FileSystemFactory.instance().getFileSystem().delete(inMapFiles[1],true);
            }

        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
}
