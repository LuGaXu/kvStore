package nju.bigdata.main;

import cn.helium.kvstore.rpc.RpcServer;
import nju.bigdata.factory.AddressIndexHelper;
import nju.bigdata.factory.FileSystemFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;

import java.io.IOException;

public class MergeLogFileThread implements Runnable{

    private int mergeKey;

    public MergeLogFileThread(int mergeKey){
        this.mergeKey=mergeKey;
    }

    @Override
    public void run() {
        merge();
    }

    private synchronized void merge(){
        AddressIndexHelper helper= AddressIndexHelper.getInstance();

        Path[] inMapFiles=new Path[4];
        String serverPath= FileSystemFactory.instance().getServerPath();
        Path logFile1=new Path(serverPath+"/"+helper.getAddress(mergeKey-1));
        inMapFiles[0]=logFile1;
        Path logFile2=new Path(serverPath+"/"+helper.getAddress(mergeKey-2));
        inMapFiles[1]=logFile2;
        Path logFile3=new Path(serverPath+"/"+helper.getAddress(mergeKey-3));
        inMapFiles[2]=logFile3;
        Path logFile4=new Path(serverPath+"/"+helper.getAddress(mergeKey-4));
        inMapFiles[3]=logFile4;

        String outputFileName = "" + RpcServer.getRpcServerId() + "_" + System.currentTimeMillis();
        Path outMapFile=new Path(serverPath+"/"+outputFileName);

        try {
            MapFile.Merger merger=new MapFile.Merger(FileSystemFactory.instance().getFileSystem().getConf());
            merger.merge(inMapFiles,true,outMapFile);//删除原来的四个日志文件
        } catch (IOException e) {
            e.printStackTrace();
        }

        //更新日志文件位置表
        helper.addAddressIndex(mergeKey,outputFileName);

    }
}
