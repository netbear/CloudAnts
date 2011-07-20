package voldemort.store.tracker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import voldemort.client.ClientConfig;
import voldemort.client.protocol.RequestFormatType;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.server.RequestRoutingType;
import voldemort.store.Store;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.socket.SocketDestination;
import voldemort.store.socket.SocketPool;
import voldemort.store.socket.SocketStore;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.Utils;
import voldemort.versioning.ClockEntry;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import com.google.common.collect.Maps;

public class VersionMonitor {

    private static final Logger logger = Logger.getLogger(VersionMonitor.class.getName());
    private MetadataStore metaStore;
    private final SocketPool socketPool;
    private Map<Integer, Store<ByteArray, byte[]>> serverMapping;
    private static final ThreadLocal<String> clientAddr = new ThreadLocal<String>();
    public VersionMonitorServer monitorServer;

    public VersionMonitor(MetadataStore meta, ClientConfig config) {
        metaStore = meta;
        socketPool = new SocketPool(config.getMaxConnectionsPerNode(),
                                    config.getConnectionTimeout(TimeUnit.MILLISECONDS),
                                    config.getSocketTimeout(TimeUnit.MILLISECONDS),
                                    config.getSocketBufferSize(),
                                    config.getSocketKeepAlive());
        serverMapping = Maps.newHashMap();
        monitorServer = VersionMonitorServer.getServerInstance(meta, 6600, 10, 1000, 64000);
        init();
    }

    public static String getAddr() {
        return clientAddr.get();
    }

    public static void setAddr(String addr) {
        clientAddr.set(addr);
    }

    public void init() {
        Cluster cluster = metaStore.getCluster();
        for(Node node: cluster.getNodes())
            if(node.getId() != metaStore.getNodeId()) {
                String host = node.getHost();
                int port = node.getSocketPort();
                Store<ByteArray, byte[]> store = new SocketStore(MetadataStore.METADATA_STORE_NAME,
                                                                 new SocketDestination(Utils.notNull(host),
                                                                                       port,
                                                                                       RequestFormatType.VOLDEMORT_V2),
                                                                 socketPool,
                                                                 RequestRoutingType.IGNORE_CHECKS);
                serverMapping.put(node.getId(), store);
            }
        if(!monitorServer.isAlive())
            monitorServer.start();
    }

    public Versioned<byte[]> getClusterVersion() {
        List<ClockEntry> versions = new ArrayList<ClockEntry>();
        for(Integer nodeId: serverMapping.keySet()) {
            ByteArray key = new ByteArray(ByteUtils.getBytes(MetadataStore.SERVER_VERSION, "UTF-8"));
            String serverVersion = ByteUtils.getString(serverMapping.get(nodeId)
                                                                    .get(key)
                                                                    .get(0)
                                                                    .getValue(), "UTF-8");
            versions.add(new ClockEntry(nodeId.shortValue(), Long.parseLong(serverVersion)));
        }
        String localVersion = ByteUtils.getString(metaStore.get(MetadataStore.SERVER_VERSION)
                                                           .get(0)
                                                           .getValue(), "UTF-8");
        versions.add(new ClockEntry((short) metaStore.getNodeId(), Long.parseLong(localVersion)));
        Collections.sort(versions, new Comparator<ClockEntry>() {

            public int compare(ClockEntry c1, ClockEntry c2) {
                if(c1.getNodeId() < c2.getNodeId())
                    return -1;
                else if(c1.getNodeId() == c2.getNodeId())
                    return 0;
                else
                    return 1;
            }
        });

        VectorClock version = new VectorClock(versions, System.currentTimeMillis());
        Versioned<byte[]> rValue = new Versioned<byte[]>(ByteUtils.getBytes(localVersion, "UTF-8"),
                                                         version);
        return rValue;
    }

}
