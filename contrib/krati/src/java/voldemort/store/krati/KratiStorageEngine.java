package voldemort.store.krati;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import krati.cds.array.DataArray;
import krati.cds.impl.segment.MemorySegmentFactory;
import krati.cds.impl.segment.SegmentFactory;
import krati.cds.impl.store.DynamicDataStore;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.store.NoSuchCapabilityException;
import voldemort.store.StorageEngine;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreUtils;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.utils.Utils;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Occured;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class KratiStorageEngine implements StorageEngine<ByteArray, byte[]> {

    private static final Logger logger = Logger.getLogger(KratiStorageEngine.class);
    private final String name;

    private DynamicDataStore datastore = null;

    public KratiStorageEngine(String name,
                              int segmentFileSizeMB,
                              double hashLoadFactor,
                              int initLevel,
                              File dataDirectory) {
        this.name = Utils.notNull(name);
        SegmentFactory segmentFactory = new MemorySegmentFactory();

        try {
            datastore = new DynamicDataStore(dataDirectory,
                                             initLevel,
                                             segmentFileSizeMB,
                                             segmentFactory,
                                             hashLoadFactor);
        } catch(Exception e) {
            logger.error("Failed to initialize datastore");
            datastore = null;
        }

    }

    public Object getCapability(StoreCapabilityType capability) {
        throw new NoSuchCapabilityException(capability, getName());
    }

    public String getName() {
        return this.name;
    }

    public void close() throws VoldemortException {}

    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);
        return StoreUtils.getAll(this, keys);
    }

    public List<Version> getVersions(ByteArray key) {
        return StoreUtils.getVersions(get(key));
    }

    public void truncate() {
        try {
            datastore.clear();
        } catch(Exception e) {
            logger.error(e);
            throw new VoldemortException("Failed to truncate Krati");
        }
    }

    public List<Versioned<byte[]>> get(ByteArray key) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        try {
            return disassembleValues(datastore.get(key.get()));
        } catch(Exception e) {
            logger.error(e);
            throw new VoldemortException("Failed to deserialize data");
        }
    }

    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries() {
        List<Pair<ByteArray, Versioned<byte[]>>> returnedList = new ArrayList<Pair<ByteArray, Versioned<byte[]>>>();
        DataArray array = datastore.getDataArray();
        for(int index = 0; index < array.length(); index++) {
            byte[] returnedBytes = array.getData(index);
            if(returnedBytes != null) {
                // Extract the key value pair from this
                // TODO: Move to DynamicDataStore code
                ByteBuffer bb = ByteBuffer.wrap(returnedBytes);
                int cnt = bb.getInt();
                if(cnt > 0) {
                    int keyLen = bb.getInt();
                    byte[] key = new byte[keyLen];
                    bb.get(key);

                    int valueLen = bb.getInt();
                    byte[] value = new byte[valueLen];
                    bb.get(value);

                    List<Versioned<byte[]>> versions;
                    try {
                        versions = disassembleValues(value);
                    } catch(IOException e) {
                        versions = null;
                    }

                    if(versions != null) {
                        Iterator<Versioned<byte[]>> iterVersions = versions.iterator();
                        while(iterVersions.hasNext()) {
                            Versioned<byte[]> currentVersion = iterVersions.next();
                            returnedList.add(Pair.create(new ByteArray(key), currentVersion));
                        }
                    }

                }
            }

        }
        return new KratiClosableIterator(returnedList);
    }

    public ClosableIterator<ByteArray> keys() {
        return StoreUtils.keys(entries());
    }

    public boolean delete(ByteArray key, Version maxVersion) throws VoldemortException {
        StoreUtils.assertValidKey(key);

        if(maxVersion == null) {
            try {
                return datastore.delete(key.get());
            } catch(Exception e) {
                logger.error(e);
                throw new VoldemortException("Failed to delete key" + key);
            }
        }

        List<Versioned<byte[]>> returnedValuesList = this.get(key);

        // Case if there is nothing to delete
        if(returnedValuesList.size() == 0) {
            return false;
        }

        Iterator<Versioned<byte[]>> iter = returnedValuesList.iterator();
        while(iter.hasNext()) {
            Versioned<byte[]> currentValue = iter.next();
            Version currentVersion = currentValue.getVersion();
            if(currentVersion.compare(maxVersion) == Occured.BEFORE) {
                iter.remove();
            }

        }

        try {
            if(returnedValuesList.size() == 0) {
                List<Versioned<byte[]>> genList = this.get(key);
                return datastore.delete(key.get());
            } else {
                return datastore.put(key.get(), assembleValues(returnedValuesList));
            }
        } catch(Exception e) {
            logger.error(e);
            throw new VoldemortException("Failed to delete key " + key);
        }
    }

    public void put(ByteArray key, Versioned<byte[]> value) throws VoldemortException {
        StoreUtils.assertValidKey(key);

        // First get the value
        List<Versioned<byte[]>> existingValuesList = this.get(key);

        // If no value, add one
        if(existingValuesList.size() == 0) {

            existingValuesList = new ArrayList<Versioned<byte[]>>();
            existingValuesList.add(new Versioned<byte[]>(value.getValue(), value.getVersion()));

        } else {

            // Update the value
            List<Versioned<byte[]>> removedValueList = new ArrayList<Versioned<byte[]>>();
            for(Versioned<byte[]> versioned: existingValuesList) {
                Occured occured = value.getVersion().compare(versioned.getVersion());

                if(occured == Occured.BEFORE) {
                    throw new ObsoleteVersionException("Obsolete version for key '" + key + "': "
                                                       + value.getVersion());
                } else if(occured == Occured.AFTER) {
                    removedValueList.add(versioned);
                }

            }
            existingValuesList.removeAll(removedValueList);
            existingValuesList.add(value);

        }

        try {
            datastore.put(key.get(), assembleValues(existingValuesList));
        } catch(Exception e) {
            logger.error(e);
            throw new VoldemortException("Failed to put key " + key);
        }
    }

    /**
     * Store the versioned values
     * 
     * @param values list of versioned bytes
     * @return the list of versioned values rolled into an array of bytes
     */
    private byte[] assembleValues(List<Versioned<byte[]>> values) throws IOException {

        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        DataOutputStream dataStream = new DataOutputStream(stream);

        for(Versioned<byte[]> value: values) {
            byte[] object = value.getValue();
            dataStream.writeInt(object.length);
            dataStream.write(object);

            VectorClock clock = (VectorClock) value.getVersion();
            dataStream.writeInt(clock.sizeInBytes());
            dataStream.write(clock.toBytes());
        }

        return stream.toByteArray();
    }

    /**
     * Splits up value into multiple versioned values
     * 
     * @param value
     * @return
     * @throws IOException
     */
    private List<Versioned<byte[]>> disassembleValues(byte[] values) throws IOException {

        if(values == null)
            return new ArrayList<Versioned<byte[]>>(0);

        List<Versioned<byte[]>> returnList = new ArrayList<Versioned<byte[]>>();
        ByteArrayInputStream stream = new ByteArrayInputStream(values);
        DataInputStream dataStream = new DataInputStream(stream);

        while(dataStream.available() > 0) {
            byte[] object = new byte[dataStream.readInt()];
            dataStream.read(object);

            byte[] clockBytes = new byte[dataStream.readInt()];
            dataStream.read(clockBytes);
            VectorClock clock = new VectorClock(clockBytes);

            returnList.add(new Versioned<byte[]>(object, clock));
        }

        return returnList;
    }

    private class KratiClosableIterator implements
            ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> {

        private Iterator<Pair<ByteArray, Versioned<byte[]>>> iter;

        public KratiClosableIterator(List<Pair<ByteArray, Versioned<byte[]>>> list) {
            iter = list.iterator();
        }

        public void close() {
        // Nothing to close here
        }

        public boolean hasNext() {
            return iter.hasNext();
        }

        public Pair<ByteArray, Versioned<byte[]>> next() {
            return iter.next();
        }

        public void remove() {
            Pair<ByteArray, Versioned<byte[]>> currentPair = next();
            delete(currentPair.getFirst(), currentPair.getSecond().getVersion());
        }

    }
}
