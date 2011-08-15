package voldemort.store.tracker;

import org.apache.log4j.Logger;

import voldemort.client.ClientConfig;
import voldemort.server.AbstractService;
import voldemort.server.ServiceType;
import voldemort.store.metadata.MetadataStore;

public class VersionMonitor extends AbstractService {

    private static final Logger logger = Logger.getLogger(VersionMonitor.class.getName());

    private static final ThreadLocal<String> clientAddr = new ThreadLocal<String>();
    public VersionMonitorServer monitorServer;

    public VersionMonitor(MetadataStore meta, ClientConfig config) {
        super(ServiceType.VERSION_MONITOR);
        monitorServer = VersionMonitorServer.getServerInstance(meta, 6600, 10, 1000, 64000);
    }

    public static String getAddr() {
        return clientAddr.get();
    }

    public static void setAddr(String addr) {
        clientAddr.set(addr);
    }

    @Override
    protected void startInner() {
        monitorServer.start();
    }

    @Override
    protected void stopInner() {
        monitorServer.shutdown();
    }

}
