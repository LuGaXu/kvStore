package nju.bigdata.main;

import cn.helium.kvstore.common.KvStoreConfig;
import cn.helium.kvstore.processor.Processor;
import cn.helium.kvstore.rpc.RpcClientFactory;
import cn.helium.kvstore.rpc.RpcServer;
import nju.bigdata.factory.AddressIndexHelper;
import nju.bigdata.factory.FileSystemFactory;
import nju.bigdata.factory.KvMapFactory;
import nju.bigdata.factory.LogFileFactory;
import nju.bigdata.model.KvConstant;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import java.io.*;
import java.util.*;

public class MockProcessor implements Processor {

    //static String serverPath="hdfs://114.212.242.156:9000/";
    //static FileSystem fs = null;

    public MockProcessor() {
        super();

        //FileSystemFactory.instance().getFileSystem();
        //TODO 读取log
        LogFileFactory.instance().readLogFiles();
//        Thread t=new Thread(new MonitorFileSizeThread());
//        t.start();
    }

    @Override
    public Map<String, String> get(String key) {

        //先在自己的map里查找
        KvMapFactory kvMapFactory = KvMapFactory.getInstance();

        if (kvMapFactory.getCurrentKvMap().containsKey(key)) {
            return (Map<String, String>) kvMapFactory.getCurrentKvMap().get(key);
        } else {
            Map<String, String> value;

            //然后根据索引在hdfs查找
            value = getValueFromHDFS(key);
            if (value != null) {
                return value;
            }

            //最后向其它kvpod发送请求，进行查找
            String valueStr = sendMessage(key);

            if (valueStr.equals("find nothing.")) {
                return null;
            } else if (valueStr.endsWith(",")) {
                valueStr = valueStr.substring(0, valueStr.length() - 1);
            }

            String[] vArray = valueStr.split(",");

            String[] tmp;
            value = new HashMap<String, String>();
            for (int i = 0; i < vArray.length; i++) {
                tmp = vArray[i].split("=");
                value.put(tmp[0], tmp[1]);
            }

            return value;
        }
    }

    /*
    根据索引在hdfs查找
     */
    private Map<String, String> getValueFromHDFS(String key) {
        //从获取索引文件位置
        AddressIndexHelper helper = AddressIndexHelper.getInstance();
        String indexFileName = helper.getAddress(0);
        String indexFilePath = FileSystemFactory.instance().getServerPath() + "/" + indexFileName;

        try {
            //如果索引文件不存在
            if (!FileSystemFactory.instance().getFileSystem().exists(new Path(indexFilePath))) {
                return null;
            }

            MapFile.Reader reader = new MapFile.Reader(new Path(indexFilePath), FileSystemFactory.instance().getFileSystem().getConf());

            if (!reader.seek(new BytesWritable(key.getBytes()))) {
                return null;
            }

            Writable result = reader.get(new BytesWritable(key.getBytes()), new IntWritable());

            int logFileKey = ((IntWritable) result).get();
            reader.close();

            //从内存中获取日志文件的位置
            String logFileName = helper.getAddress(logFileKey);
            String logFilePath = FileSystemFactory.instance().getServerPath() + "/" + logFileName;

            //从日志文件中获取valuemap
            MapFile.Reader logFileReader = new MapFile.Reader(new Path(logFilePath), FileSystemFactory.instance().getFileSystem().getConf());
            Writable valueResult = logFileReader.get(new BytesWritable(key.getBytes()), new MapWritable());

            if (valueResult != null) {
                Map<String, String> value = new HashMap<String, String>();
                for (Map.Entry<Writable, Writable> entry : ((MapWritable) valueResult).entrySet()) {
                    value.put(entry.getKey().toString().trim(), entry.getValue().toString().trim());
                }
                logFileReader.close();

                //将键值对加入内存中的map
                KvMapFactory kvMapFactory = KvMapFactory.getInstance();
                kvMapFactory.getCurrentKvMap().put(key, value);

                return value;
            }
            logFileReader.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    private String getValueStr(String key) {
        KvMapFactory kvMapFactory = KvMapFactory.getInstance();
        Map<String, String> value = (Map<String, String>) kvMapFactory.getCurrentKvMap().get(key);

        if (value == null) {
            value = getValueFromHDFS(key);
        }

        String valueStr = "";
        if (value != null) {
            for (String k : value.keySet()) {
                valueStr = valueStr + k + "=" + value.get(k) + ",";
            }

            return valueStr;
        } else {
            return null;
        }

    }

    @Override
    public synchronized boolean put(String key, Map<String, String> value) {
        return putOne(key,value);
    }

    private boolean writeIntoHDFS() {
        boolean isSuccess = true;
        String logFile = LogFileFactory.instance().getCurrentLogPath();
        if (LogFileFactory.instance().isFull()) {
            isSuccess = KvMapFactory.getInstance().changeCurrent();
            if (isSuccess) {
                //创建线程，写入HDFS
                Thread thread = new Thread(new WriteIntoHDFSThread(logFile,KvMapFactory.getInstance().mapToBeWritten()));
                thread.start();
            }
        }
        return isSuccess;

    }

    @Override
    public synchronized boolean batchPut(Map<String, Map<String, String>> records) {
        //键-值追加写入log文件
        try {
            BufferedWriter bw = LogFileFactory.instance().getBufferedWriter();//到时候返回BufferedWriter的instance就好了叭
            if (bw==null)
                return false;

            for (Map.Entry<String, Map<String, String>> entry : records.entrySet()) {
                String str=entry.getKey()+entry.getValue().toString()+ KvConstant.LINE_SEPERATOR;
                bw.write(str);
                KvMapFactory.getInstance().put(entry.getKey(), entry.getValue());
            }
            bw.flush();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return false;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        //文件满，则将键-值及其索引存入HDFS
        boolean isSuccess = writeIntoHDFS();
        //boolean isSuccess=false;
        return isSuccess;
    }

    private  boolean putOne(String key, Map<String, String> value){
        //键-值追加写入log文件
        try {
            BufferedWriter bw = LogFileFactory.instance().getBufferedWriter();//到时候返回BufferedWriter的instance就好了叭
            if (bw==null)
                return false;

            String str=key+value.toString()+ KvConstant.LINE_SEPERATOR;

            bw.write(str);

            bw.flush();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return false;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        //键-值存入内存
//        KvMapFactory kvMapFactory = KvMapFactory.getInstance();
        KvMapFactory.getInstance().put(key, value);

        //文件满，则将键-值及其索引存入HDFS
        boolean isSuccess = writeIntoHDFS();

        return isSuccess;
    }

    @Override
    public int count(Map<String, String> map) {
        return 0;
    }

    @Override
    public Map<Map<String, String>, Integer> groupBy(List<String> list) {
        return null;
    }

    @Override
    public byte[] process(byte[] input) {
        //System.out.println("receive info:   " + new String( input ));
        //TODO 调用private get
        String keyStr = new String(input);
        String valueStr = getValueStr(keyStr);

        if (valueStr == null) {
            return "find nothing.".getBytes();
        } else {
            return valueStr.getBytes();
        }

    }

    //暂定只有get
    private String sendMessage(String message) {
        int id = RpcServer.getRpcServerId();
        int num_pod = KvStoreConfig.getServersNum();
        byte[] temp = null;
        String result = "find nothing.";
        int i = 0;

        while (i < num_pod) {
            if (i != id) {
                try {
                    temp = RpcClientFactory.inform(i, message.getBytes());
                    if (new String(temp).equals("find nothing.")) {
                        i++;
                    } else {
                        result = new String(temp);
                        break;
                    }
                } catch (IOException o) {
                    i++;
                }
            } else {
                i++;
            }
        }
        return result;
    }
}
