package nju.bigdata.main;

import nju.bigdata.factory.LogFileFactory;

import java.io.File;

/**
 * Created by lujxu on 2017/11/6.
 */
public class MonitorFileSizeThread implements Runnable {
    @Override
    public void run() {
        while (true){

        }
       /* while (true) {
            String currentLog = LogFileFactory.instance().getCurrentLogPath();
            double size = getFileSize(currentLog);
            LogFileFactory.instance().setFileSize(size);
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            //LogFileFactory.instance().setCurrentLog("hello");
            //System.out.println("in MonitorFileSizeThread: "+LogFileFactory.instance().getCurrentLogPath());

        }*/
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
}
