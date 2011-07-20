package voldemort.store.tracker;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.ClientConfig;
import voldemort.store.DelegatingStore;
import voldemort.store.Store;
import voldemort.store.metadata.MetadataStore;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import com.google.common.collect.Maps;

public class TrackStore<K, V> extends DelegatingStore<K, V> {

    private static final Logger logger = Logger.getLogger(TrackStore.class.getName());
    private Set<K> newKeySet;
    private Map<K, String> keySources;
    private final MetadataStore metaData;
    private static final String CLUSTER_VERSION = "server.version";
    private VersionMonitor monitor;
    private Object keyCacheLock = new Object();
    private int nodeId;

    /**
     * Create a BackTracker
     * 
     * @param innerStore the store we wrap
     */
    public TrackStore(Store<K, V> innerStore, MetadataStore metaStore) {
        super(innerStore);
        // Try not to synchronize here
        newKeySet = Collections.synchronizedSet(new HashSet<K>());
        // newKeySet = new HashSet<K>();
        this.metaData = metaStore;
        monitor = new VersionMonitor(metaData, new ClientConfig());
        nodeId = metaData.getNodeId();
        keySources = Maps.newHashMap();
    }

    @Override
    public List<Versioned<V>> get(K key) throws VoldemortException {
        List<Versioned<V>> rValue;
        Versioned<byte[]> version;
        try {
            rValue = super.get(key);
        } catch(VoldemortException e) {
            throw e;
        }

        if(newKeySet.contains(key)) {
            synchronized(keyCacheLock) {
                if(newKeySet.contains(key)) {

                    version = monitor.getClusterVersion();
                    VectorClock clock = (VectorClock) version.getVersion();
                    clock.incrementVersion(nodeId, System.currentTimeMillis());

                    for(K k: newKeySet) {
                        String addr = keySources.get(k);
                        logger.info(addr
                                    + " Put "
                                    + k
                                    + " "
                                    + Arrays.toString(((VectorClock) version.getVersion()).toBytes()));
                    }

                    newKeySet.clear();
                    keySources.clear();
                    /*
                     * TODO: Write back server version
                     */
                    long serverVersion = Long.parseLong(ByteUtils.getString(version.getValue(),
                                                                            "UTF-8"));
                    serverVersion++;

                    ByteArray keyBytes = new ByteArray(ByteUtils.getBytes(MetadataStore.SERVER_VERSION,
                                                                          "UTF-8"));
                    Versioned<byte[]> newVersion = new Versioned<byte[]>(ByteUtils.getBytes(Long.toString(serverVersion),
                                                                                            "UTF-8"),
                                                                         clock);
                    metaData.put(keyBytes, newVersion);
                } else
                    version = metaData.get(CLUSTER_VERSION).get(0);
            }
        } else {

            version = metaData.get(CLUSTER_VERSION).get(0);
        }

        String addr = VersionMonitor.getAddr();
        logger.info(addr + " Get " + key + " "
                    + Arrays.toString(((VectorClock) version.getVersion()).toBytes()));
        System.out.println("VersionMonitorServer : " + monitor.monitorServer.getClusterVersion());

        return rValue;
    }

    @Override
    public void put(K key, Versioned<V> value) throws VoldemortException {
        try {
            super.put(key, value);
        } catch(VoldemortException e) {
            throw e;
        }

        String addr = VersionMonitor.getAddr();

        synchronized(keyCacheLock) {
            newKeySet.add(key);
            keySources.put(key, addr);
            monitor.monitorServer.incLocalClock();
            System.out.println("new clock:" + monitor.monitorServer.getLocalClock());
        }
    }
}
