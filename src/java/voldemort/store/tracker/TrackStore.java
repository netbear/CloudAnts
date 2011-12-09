package voldemort.store.tracker;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.store.DelegatingStore;
import voldemort.store.Store;
import voldemort.store.metadata.MetadataStore;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import com.google.common.collect.Maps;

public class TrackStore<K, V> extends DelegatingStore<K, V> {

    private class KeyAccess {

        public String addr;
        public long clock;

        public KeyAccess(String addr, long clock) {
            this.addr = addr;
            this.clock = clock;
        }
    }

    private static final Logger logger = Logger.getLogger(TrackStore.class.getName());

    private Map<K, KeyAccess> keyUpdates;
    private final MetadataStore metaData;

    private Object keyCacheLock = new Object();
    private int nodeId;
    private Timer timer;

    /**
     * Create a BackTracker
     * 
     * @param innerStore the store we wrap
     */
    public TrackStore(Store<K, V> innerStore, MetadataStore metaStore) {
        super(innerStore);
        // Try not to synchronize here
        this.metaData = metaStore;
        // monitor = new VersionMonitor(metaData, new ClientConfig());
        nodeId = metaData.getNodeId();
        keyUpdates = Maps.newLinkedHashMap();

        TimerTask scheduleTask = new TimerTask() {

            @Override
            public void run() {
                synchronized(keyCacheLock) {
                    flushPutRecords();
                }
            }
        };

        timer = new Timer();
        timer.schedule(scheduleTask, 1000, 1000);
    }

    /*
     * This method is not synchronized, caller should provide guarantees that
     * execution is inside particular critical section.
     */
    public void flushPutRecords() {
        if(keyUpdates.isEmpty())
            return;

        VersionMonitorServer mServer = VersionMonitorServer.getServerInstance();
        if(mServer == null)
            throw new VoldemortException("Version Monitor Server should not be null!");
        mServer.updateClusterClock();
        Iterator<Entry<K, KeyAccess>> iter = keyUpdates.entrySet().iterator();
        while(iter.hasNext()) {
            Entry<K, KeyAccess> ac = iter.next();
            VectorClock version = mServer.getClusterVersion(ac.getValue().clock);
            logger.info(ac.getValue().addr + " Put " + ac.getKey() + " " + version);
        }
        keyUpdates.clear();
    }

    @Override
    public List<Versioned<V>> get(K key) throws VoldemortException {
        List<Versioned<V>> rValue;

        try {
            rValue = super.get(key);
        } catch(VoldemortException e) {
            throw e;
        }

        String addr = VersionMonitor.getAddr();

        if(keyUpdates.keySet().contains(key)) {
            synchronized(keyCacheLock) {
                if(keyUpdates.keySet().contains(key)) {
                    flushPutRecords();
                }
            }
        }
        VersionMonitorServer mServer = VersionMonitorServer.getServerInstance();
        if(mServer == null)
            throw new VoldemortException("Version Monitor Server should not be null!");

        logger.info(addr + " Get " + key + " " + mServer.getClusterVersion());

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
        VersionMonitorServer mServer = VersionMonitorServer.getServerInstance();
        if(mServer == null)
            throw new VoldemortException("Version Monitor Server should not be null!");

        synchronized(keyCacheLock) {

            long clock = mServer.incLocalClock();
            keyUpdates.put(key, new KeyAccess(addr, clock));
        }
    }
}
