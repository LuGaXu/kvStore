package nju.bigdata.factory;

import nju.bigdata.model.KvConstant;

import java.io.*;
import java.util.*;

/**
 * Created by lujxu on 2017/10/23.
 */
public class LogFileFactory {
    private  static LogFileFactory logFileFactory =new LogFileFactory();
 /*   private  static final String LOG1 = KvConstant.PERSIST_FILE_PATH+"/log1";
    private static final String LOG2 =KvConstant.PERSIST_FILE_PATH+"/log2";
    private  static final String LOG3 = KvConstant.PERSIST_FILE_PATH+"/log3";
    private static final String LOG4 =KvConstant.PERSIST_FILE_PATH+"/log4";*/
    private  String currentLog;
    //private  String readOnlyLog;
    private FileOutputStream fileOutputStream;
//    private ObjectOutputStream objectOutputStream;
//    private ObjectInputStream input;
//    private FileInputStream fin;
    private FileReader fr = null;
    private OutputStreamWriter outputStreamWriter;
    private BufferedReader bufReader = null;
    private BufferedWriter bufferedWriter;
    private Queue<String> fileQueue;
    //private double fileSize;
    private int count;

    private LogFileFactory(){
        //判断谁为current
        count=0;
        fileQueue=new LinkedList<>();
        initFileQueue();
        while (fileQueue.size()>=0&&getFileSize(currentLog)==0.0){
            if(currentLog==null)
                break;
            deleteLogFile(currentLog);
            currentLog=fileQueue.poll();
        }
        if(currentLog==null&&fileQueue.size()==0){
            currentLog=constructLogPath();
        }
        //fileSize = getFileSize(currentLog);
    }

    public static LogFileFactory instance() {
        return logFileFactory;
    }

    public BufferedWriter getBufferedWriter(){
        try {
            if (this.bufferedWriter == null) {
                this.outputStreamWriter = new OutputStreamWriter(getFileOutputStream());
                this.bufferedWriter = new BufferedWriter(this.outputStreamWriter);
            }
        }catch(Exception e){
            e.printStackTrace();
              return null;
        }
        return this.bufferedWriter;
    }

    public FileOutputStream getFileOutputStream() {

        if (this.fileOutputStream == null) {
            File file = new File(currentLog);
            try {
                this.fileOutputStream = new FileOutputStream(file, true);
            } catch (IOException e) {
               // e.printStackTrace();
                return null;
            }
        }
        return this.fileOutputStream;

    }

    public void closeFileOutputStream() {
        try {
              //  this.objectOutputStream.close();
               // this.objectOutputStream=null;
                this.bufferedWriter.close();
                this.bufferedWriter=null;
                this.outputStreamWriter=null;
                this.fileOutputStream = null;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void readLogFiles() {
        readLogFile(currentLog);
        if(fileQueue.size()>0){
            readLogFile(fileQueue.poll());
        }
    }

    public String getCurrentLogPath(){
        return currentLog;
    }

    /**
     * 判断文件是否满足写入条件
     * 若满足，返回true，同时切换新的文件为current
     * 否则，返回false
     * @return
     */
    public synchronized boolean isFull(){
        //if(getFileSize(currentLog)>=KvConstant.FULL_FILE_SIZE){
       /* if (this.fileSize >= KvConstant.FULL_FILE_SIZE) {
            closeFileOutputStream();
            changeCurrentLogFile();
            return true;
        }
        return false;*/
       if(KvMapFactory.getInstance().getCurrentKvMap().size()>=KvConstant.MAX_OBJECT_COUNT){
           //this.count=0;
           closeFileOutputStream();
           changeCurrentLogFile();
           return true;
       }
       return false;
    }


    /**
     * 清空文件日志
     *
     * @return
     */
    public boolean clearLogFile(String filename){
        //清空文件内容
        File file =new File(filename);
        try {
            if(!file.exists()) {
                file.createNewFile();
            }
            FileWriter fileWriter =new FileWriter(file);
            fileWriter.write("");
            fileWriter.flush();
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * 删除文件
     * @param filename
     * @return
     */
    public boolean deleteLogFile(String filename){
        File file = new File(filename);
        // 如果文件路径所对应的文件存在，并且是一个文件，则直接删除
        if (file.exists() && file.isFile()) {
            if (file.delete()) {
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    /**
     * calculate the size of file in the disk
     *
     * @return MB
     */
    private double getFileSize(String filename) {
        if (filename == null || filename.isEmpty())
            return 0.0;
        File f = new File(filename);
        if (f.exists() && f.isFile()) {
            return f.length() / 1024.0 / 1024.0;
        }
        return 0.0;
    }

    private void readLogFile(String filename){
        File logFile=new File(filename);
        if(!logFile.exists()){
            try {
                logFile.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }finally {
                return;
            }
        }
        try {
            fr = new FileReader(logFile);
            bufReader = new BufferedReader(fr);

            KvMapFactory kvMapFactory=KvMapFactory.getInstance();

            String temp = null;
            String[] tmp;
            String[] values;
            while ((temp = bufReader.readLine()) != null) {
                tmp = temp.split("\\{");
                String key=tmp[0].trim();

                tmp[1]=tmp[1].replace('}',' ');
                values=tmp[1].split(",");

                Map<String,String> valueMap=new HashMap<String,String>();
                String[] k_v;
                for(int i=0;i<values.length;i++){
                    k_v=values[i].split("=");
                    valueMap.put(k_v[0].trim(),k_v[1].trim());
                }

                kvMapFactory.getCurrentKvMap().put(key,valueMap);
            }

            fr.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void  initFileQueue(){
        /*fileQueue.offer(LOG1);
        fileQueue.offer(LOG2);
        fileQueue.offer(LOG3);
        fileQueue.offer(LOG4);*/
        findAllLog();
        if(fileQueue.size()<=0){
            currentLog=constructLogPath();
            return;
        }
        currentLog=fileQueue.poll();
    }

    /**
     * 找到/opt/localdisk下的所有log文件并读取
     */
    private void findAllLog(){
        File f=new File(KvConstant.PERSIST_FILE_PATH);
        String list1[]=f.list();
        for (String filename:list1){
            if(filename.startsWith("log")){
                StringBuilder builder=new StringBuilder(KvConstant.PERSIST_FILE_PATH);
                builder.append("/");
                builder.append(filename);
                fileQueue.offer(builder.toString());
            }
        }
    }

    private void changeCurrentLogFile(){
        if (fileQueue.size()>0){
            currentLog=fileQueue.poll();
            return;
        }
        currentLog=constructLogPath();
//        if(currentLog==null)
//            initFileQueue();
       // this.fileSize = 0.0;
    }

    /**
     * 生成随机log路径
     * @return
     */
    private String constructLogPath(){
        StringBuilder builder=new StringBuilder(KvConstant.PERSIST_FILE_PATH);
        builder.append("/log");
        builder.append(System.currentTimeMillis());
        return builder.toString();
    }

    public static void main(String []args){

        File f=new File(KvConstant.PERSIST_FILE_PATH);
        String list1[]=f.list();
        for (String filename:list1){
            if(filename.startsWith("log")){
               File f1=new File(filename);
               f1.delete();
            }
        }
    }
}
