package nju.bigdata.factory;

import cn.helium.kvstore.common.KvStoreConfig;
import cn.helium.kvstore.common.Parameters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URI;

/**
 * Created by lujxu on 2017/10/22.
 */
public class FileSystemFactory {
    private  static  FileSystemFactory fileSystemFactory=new FileSystemFactory();
    private  static FileSystem fileSystem;
    private  static  String  serverPath;
    private  static Configuration configuration;

    private FileSystemFactory(){
        config();
        serverPath=KvStoreConfig.getHdfsUrl();
        serverPath+="/user/hadoop";
        configuration=new Configuration();
        configuration.setInt("dfs.block.size", 67108864);
    }

    public static FileSystemFactory instance(){
        return fileSystemFactory;
    }

    public  FileSystem getFileSystem(){
        if(fileSystem==null){
            try {
                fileSystem=FileSystem.get(URI.create(serverPath), configuration);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return fileSystem;
    }

    public String getServerPath(){
        return serverPath;
    }

    private void config(){

        Parameters parameters = new Parameters();

        try {
            KvStoreConfig.loadConfig(parameters);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected void finalize(){
        try {
            super.finalize();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
        try {
            fileSystem.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
