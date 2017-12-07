package nju.bigdata.factory;

import nju.bigdata.main.MergeLogFileThread;

import java.util.Observable;
import java.util.Observer;

/**
 * Created by lujxu on 2017/11/2.
 */
public class MergeLogObserver implements Observer {

    public  MergeLogObserver(AddressIndexHelper addressIndexHelper){
        super();
        addressIndexHelper.addObserver(this);
    }
    @Override
    public void update(Observable o, Object arg) {
        String str=arg.toString().trim();
        try {
            int mergeKey=Integer.parseInt(str);
            Thread t=new Thread(new MergeLogFileThread(mergeKey));
            t.start();
        }catch (NumberFormatException e){
            e.printStackTrace();
            return;
        }
    }

}
