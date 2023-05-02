package datawave.query.tables.listener;

import com.google.common.util.concurrent.Service;
import datawave.query.tables.stats.ScanSessionStats;

public class ScannerSessionListener extends Service.Listener{

    @Override
    public void stopping(Service.State from) {
        System.out.println("Stopping from " + from);
   }

    @Override
    public void terminated(Service.State from) {
        System.out.println("terminated from " + from);
    }

    /*
     * (non-Javadoc)
     *
     * @see com.google.common.util.concurrent.Service.Listener#failed(com.google.common.util.concurrent.Service.State, java.lang.Throwable)
     */
    @Override
    public void failed(Service.State from, Throwable failure) {
        System.out.println("failed from " + from + " " + failure.getMessage());
    }
}
