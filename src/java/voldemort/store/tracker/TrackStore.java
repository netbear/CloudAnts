package voldemort.store.tracker;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
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

public class TrackStore<K, V> extends DelegatingStore<K, V> {

    private static final Logger logger = Logger.getLogger(TrackStore.class.getName());
    private Set<K> newKeySet;
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
        newKeySet = Collections.synchronizedSet(new HashSet<K>());
        this.metaData = metaStore;
        monitor = new VersionMonitor(metaData, new ClientConfig());
        nodeId = metaData.getNodeId();
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
                version = monitor.getClusterVersion();
                for(K k: newKeySet) {
                    logger.info("Put " + k + " " + version.getVersion());
                }
                VectorClock clock = (VectorClock) version.getVersion();
                clock.incrementVersion(nodeId, System.currentTimeMillis());
                newKeySet.clear();
                /*
                 * TODO: Write back server version
                 */
                long serverVersion = Long.parseLong(ByteUtils.getString(version.getValue(), "UTF-8"));
                serverVersion++;

                ByteArray keyBytes = new ByteArray(ByteUtils.getBytes(MetadataStore.SERVER_VERSION,
                                                                      "UTF-8"));
                Versioned<byte[]> newVersion = new Versioned<byte[]>(ByteUtils.getBytes(Long.toString(serverVersion),
                                                                                        "UTF-8"),
                                                                     clock);
                metaData.put(keyBytes, newVersion);
            }
        } else {

            version = metaData.get(CLUSTER_VERSION).get(0);
        }

        logger.info("Get " + key + " " + version.getVersion());

        return rValue;
    }

    @Override
    public void put(K key, Versioned<V> value) throws VoldemortException {
        try {
            super.put(key, value);
        } catch(VoldemortException e) {
            throw e;
        }

        synchronized(keyCacheLock) {
            newKeySet.add(key);
        }
    }
}
