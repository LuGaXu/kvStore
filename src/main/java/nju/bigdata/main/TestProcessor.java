package nju.bigdata.main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.logging.log4j.core.config.plugins.util.ResolverUtil;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

/**
 * Created by lujxu on 2017/10/27.
 */
public class TestProcessor {
    static Map<String, Map <String, String>> store = new HashMap<>();
    static Configuration conf = new Configuration();
    static String serverPath = "hdfs://114.212.242.156:9000/user/hadoop/";
    static FileSystem fs = null;

    static Hashtable<String,String> keyTable=new Hashtable();//key和位置的哈希表，位置暂时用文件名替代
    static Map<String, Map<String, String>> totalRecords=new HashMap<>();//暂时存放在内存中的所有k-v记录

    public static void main(String[] args) {
      //  mapPut();
     //   mergeFile();
     //   readMap();
      //  System.out.println(new String(new BytesWritable(("asdas2338huh".getBytes())).getBytes()));
      //  TestProcessor.deleteFile("100,101,102,110,111");
      //  TreeMap
        String temp="E://视频/Buddies.In.India.2017.WEB-DL.1080p.H264.AAC-MTTV.mp4";

        long t1=System.currentTimeMillis();
        for(int i=0;i<1000000;i++){
            TestProcessor.getFileSize(temp);
            //TestProcessor.count();
        }
        long t2=System.currentTimeMillis();
        System.out.println(t2-t1);
    }
    static int a=0;
    public static  int count(){
            return  ++a;
    }

    public static void mapPut(){
        String key1="asdas2338huh";
        Map<String, String> value1=new HashMap<String, String>();
        value1.put("name","luga");
        value1.put("age","20");
        value1.put("tel","123123213");
        value1.put("address","HanKou Road");
        value1.put("city","Nanjing");

        String key2="hsad234jsdf";
        Map <String, String> value2=new HashMap <String, String>();
        value2.put("school","Nanjing_University");
        value2.put("address","HanKou Road");
        value2.put("tel","32323123");

        String key3="yasdasd231";
        Map <String, String> value3=new HashMap <String, String>();
        value3.put("brand","hehehe");
        value3.put("tel","2738178392");
        value3.put("add","HeHe Road");
        value3.put("2342","lalalal");

        try {
            fs = FileSystem.get(URI.create(serverPath), conf);
            Path seqFile=new Path(serverPath+"user/hadoop/mapfile5");
            //Writer内部类用于文件的写操作,假设Key和Value都为String类型
            MapFile.Writer.Option option1=MapFile.Writer.keyClass(BytesWritable.class);
            SequenceFile.Writer.Option option2=MapFile.Writer.valueClass(MapWritable.class);
            MapFile.Writer writer= new MapFile.Writer(conf,seqFile,
                    option1,option2);

            //通过writer向文档中写入记录
            BytesWritable subKey=new BytesWritable();
            subKey.set(new BytesWritable(key1.getBytes()));
            MapWritable subValue=new MapWritable();
            for(Map.Entry<String, String> entry : value1.entrySet()){

                subValue.put(new Text(entry.getKey()),new Text(entry.getValue()));
            }
            writer.append(subKey,subValue);
            subValue.clear();

            subKey.set(new BytesWritable(key2.getBytes()));
            for(Map.Entry<String, String> entry : value2.entrySet()){

                subValue.put(new Text(entry.getKey()),new Text(entry.getValue()));
            }
            writer.append(subKey,subValue);
            subValue.clear();

            subKey.set(new BytesWritable(key3.getBytes()));
            for(Map.Entry<String, String> entry : value3.entrySet()){

                subValue.put(new Text(entry.getKey()),new Text(entry.getValue()));
            }
            writer.append(subKey,subValue);

            writer.close();
            //IOUtils.closeStream(writer);//关闭write流
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static  Map<String, String>  readMap(){
        MapWritable mapWritable=null;

        try {
            fs= FileSystem.get(URI.create(serverPath), conf);
            MapFile.Reader reader=new MapFile.Reader(new Path(serverPath+"user/hadoop/mapfile7"),fs.getConf());
            Class key1 = reader.getKeyClass();
            Class value1 = reader.getValueClass();

            boolean seek = reader.seek(new BytesWritable("b38dne".getBytes()));
            Writable result=reader.get(new BytesWritable("b38dne".getBytes()), new MapWritable());
            Writable result1=reader.get(new BytesWritable("zds8adv3csa".getBytes()), new MapWritable());
            Writable result2=reader.get(new BytesWritable("ads8238huh".getBytes()), new MapWritable());

            Writable resul3=reader.get(new BytesWritable("asdas2338huh".getBytes()), new MapWritable());
            Writable result4=reader.get(new BytesWritable("yasdasd231".getBytes()), new MapWritable());
            Writable result5=reader.get(new BytesWritable("hsad234jsdf".getBytes()), new MapWritable());

            BytesWritable key2 = (BytesWritable) ReflectionUtils
                    .newInstance(key1, fs.getConf());
            MapWritable value = (MapWritable) ReflectionUtils.newInstance(
                    value1, fs.getConf());

            while(reader.next(key2,value)){
                System.out.println("key:"+key2.getBytes().toString());
                    for(Map.Entry<Writable, Writable> entry : value.entrySet()){
                        System.out.print(entry.getKey().toString()+"------");
                        System.out.println(entry.getValue().toString());
                    }
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        if(mapWritable!=null){
            Map<String,String> value=new HashMap<String,String>();
            for(Map.Entry<Writable, Writable> entry : mapWritable.entrySet()){
                value.put(entry.toString(),entry.toString());
            }
            return value;
        }
        return  null;
    }

    public  static  void mergeFile(){
        Path[] inMapFiles=new Path[2];
        inMapFiles[0]=new Path(serverPath+"user/hadoop/mapfile4");
        inMapFiles[1]=new Path(serverPath+"user/hadoop/mapfile5");
        Path outMapFile=new Path(serverPath+"user/hadoop/mapfile7");
        try {
            MapFile.Merger merger=new MapFile.Merger(conf);
            merger.merge(inMapFiles,true,outMapFile);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static  void deleteFile(String filename){
        String files[]=filename.split(",");
        for (String file:files){
            try {
                fs= FileSystem.get(URI.create(serverPath+file), conf);
                fs.delete(new Path(file),true);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static double getFileSize(String filename) {
        if (filename == null || filename.isEmpty())
            return 0.0;
        File f = new File(filename);
        if (f.exists() && f.isFile()) {
            return f.length() / 1024.0 / 1024.0;
        }
        return 0.0;
    }
}
