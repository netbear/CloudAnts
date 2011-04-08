package voldemort.store.readonly;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.TestUtils;
import voldemort.VoldemortException;
import voldemort.cluster.Node;
import voldemort.serialization.Compression;
import voldemort.serialization.SerializerDefinition;
import voldemort.store.Store;
import voldemort.utils.Utils;
import voldemort.versioning.Versioned;

import com.google.common.collect.ImmutableMap;

@RunWith(Parameterized.class)
public class ReadOnlyStorageEngineTest {

    private static int TEST_SIZE = 10;

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { new BinarySearchStrategy() },
                { new InterpolationSearchStrategy() } });
    }

    private File dir;
    private SearchStrategy strategy;
    private SerializerDefinition serDef;
    private SerializerDefinition lzfSerDef;

    public ReadOnlyStorageEngineTest(SearchStrategy strategy) {
        this.strategy = strategy;
        this.dir = TestUtils.createTempDir();
        this.serDef = new SerializerDefinition("json", "'string'");
        this.lzfSerDef = new SerializerDefinition("json",
                                                  ImmutableMap.of(0, "'string'"),
                                                  true,
                                                  new Compression("lzf", null));
    }

    @After
    public void tearDown() {
        Utils.rm(dir);
    }

    /**
     * For each key/value pair we built into the store, look it up and test that
     * the correct value is returned
     */
    @Test
    public void canGetGoodValues() throws Exception {
        ReadOnlyStorageEngineTestInstance testData = ReadOnlyStorageEngineTestInstance.create(strategy,
                                                                                              dir,
                                                                                              TEST_SIZE,
                                                                                              2,
                                                                                              2,
                                                                                              serDef,
                                                                                              serDef);
        // run test multiple times to check caching
        for(int i = 0; i < 3; i++) {
            for(Map.Entry<String, String> entry: testData.getData().entrySet()) {
                for(Node node: testData.routeRequest(entry.getKey())) {
                    Store<String, String> store = testData.getNodeStores().get(node.getId());
                    List<Versioned<String>> found = store.get(entry.getKey());
                    assertEquals("Lookup failure for '" + entry.getKey() + "' on iteration " + i
                                 + " for node " + node.getId() + ".", 1, found.size());
                    Versioned<String> obj = found.get(0);
                    assertEquals(entry.getValue(), obj.getValue());
                }
            }
        }

        testData.delete();
    }

    @Test
    public void canGetGoodCompressedValues() throws Exception {
        ReadOnlyStorageEngineTestInstance testData = ReadOnlyStorageEngineTestInstance.create(strategy,
                                                                                              dir,
                                                                                              TEST_SIZE,
                                                                                              2,
                                                                                              2,
                                                                                              serDef,
                                                                                              lzfSerDef);
        // run test multiple times to check caching
        for(int i = 0; i < 3; i++) {
            for(Map.Entry<String, String> entry: testData.getData().entrySet()) {
                for(Node node: testData.routeRequest(entry.getKey())) {
                    Store<String, String> store = testData.getNodeStores().get(node.getId());
                    List<Versioned<String>> found = store.get(entry.getKey());
                    assertEquals("Lookup failure for '" + entry.getKey() + "' on iteration " + i
                                 + " for node " + node.getId() + ".", 1, found.size());
                    Versioned<String> obj = found.get(0);
                    assertEquals(entry.getValue(), obj.getValue());
                }
            }
        }

        testData.delete();
    }

    @Test
    public void canGetGoodCompressedKeys() throws Exception {
        ReadOnlyStorageEngineTestInstance testData = ReadOnlyStorageEngineTestInstance.create(strategy,
                                                                                              dir,
                                                                                              TEST_SIZE,
                                                                                              2,
                                                                                              2,
                                                                                              lzfSerDef,
                                                                                              serDef);
        // run test multiple times to check caching
        for(int i = 0; i < 3; i++) {
            for(Map.Entry<String, String> entry: testData.getData().entrySet()) {
                for(Node node: testData.routeRequest(entry.getKey())) {
                    Store<String, String> store = testData.getNodeStores().get(node.getId());
                    List<Versioned<String>> found = store.get(entry.getKey());
                    assertEquals("Lookup failure for '" + entry.getKey() + "' on iteration " + i
                                 + " for node " + node.getId() + ".", 1, found.size());
                    Versioned<String> obj = found.get(0);
                    assertEquals(entry.getValue(), obj.getValue());
                }
            }
        }

        testData.delete();
    }

    /**
     * Do lookups on keys not in the store and test that the keys are not found.
     */
    @Test
    public void cantGetBadValues() throws Exception {
        ReadOnlyStorageEngineTestInstance testData = ReadOnlyStorageEngineTestInstance.create(strategy,
                                                                                              dir,
                                                                                              TEST_SIZE,
                                                                                              2,
                                                                                              2,
                                                                                              serDef,
                                                                                              serDef);
        // run test multiple times to check caching
        for(int i = 0; i < 3; i++) {
            for(int j = 0; j < TEST_SIZE; j++) {
                String key = TestUtils.randomLetters(10);
                if(!testData.getData().containsKey(key)) {
                    for(int k = 0; k < testData.getNodeStores().size(); k++)
                        assertEquals("Found key in store where it should not be.",
                                     0,
                                     testData.getNodeStores().get(k).get(key).size());
                }
            }
        }
        testData.delete();
    }

    @Test
    public void canMultigetGoodValues() throws Exception {
        ReadOnlyStorageEngineTestInstance testData = ReadOnlyStorageEngineTestInstance.create(strategy,
                                                                                              dir,
                                                                                              TEST_SIZE,
                                                                                              2,
                                                                                              2,
                                                                                              serDef,
                                                                                              serDef);
        Set<String> keys = testData.getData().keySet();
        Set<String> gotten = new HashSet<String>();
        for(Map.Entry<Integer, Store<String, String>> entry: testData.getNodeStores().entrySet()) {
            Set<String> queryKeys = new HashSet<String>();
            for(String key: keys)
                for(Node node: testData.routeRequest(key))
                    if(Integer.valueOf(node.getId()).equals(entry.getKey()))
                        queryKeys.add(key);
            Map<String, List<Versioned<String>>> values = entry.getValue().getAll(queryKeys);
            assertEquals("Returned fewer keys than expected.", queryKeys.size(), values.size());
            for(Map.Entry<String, List<Versioned<String>>> returned: values.entrySet()) {
                assertTrue(queryKeys.contains(returned.getKey()));
                assertEquals(1, returned.getValue().size());
                Versioned<String> val = returned.getValue().get(0);
                assertEquals(testData.getData().get(returned.getKey()), val.getValue());
                gotten.add(returned.getKey());
            }
        }
        assertEquals(keys, gotten);
        testData.delete();
    }

    @Test
    public void openInvalidStoreFails() throws Exception {
        // empty is okay
        testOpenInvalidStoreFails(0, 0, true);
        // two entries with 1 byte each of data
        testOpenInvalidStoreFails(ReadOnlyUtils.INDEX_ENTRY_SIZE * 2,
                                  ReadOnlyUtils.INDEX_ENTRY_SIZE * +2,
                                  true);

        // okay these are corrupt:
        // invalid index size
        testOpenInvalidStoreFails(73, 1024, false);
        // too little data for index (1 byte short for all empty values)
        testOpenInvalidStoreFails(ReadOnlyUtils.INDEX_ENTRY_SIZE * 10, 10 * 4 - 1, false);
        // empty index implies no data
        testOpenInvalidStoreFails(ReadOnlyUtils.INDEX_ENTRY_SIZE, 0, false);
    }

    public void testOpenInvalidStoreFails(int indexBytes, int dataBytes, boolean shouldWork)
            throws Exception {
        File versionDir = new File(dir, "version-0");
        createStoreFiles(versionDir, indexBytes, dataBytes, 2);

        try {
            new ReadOnlyStorageEngine("test", strategy, dir, 1);
            if(!shouldWork)
                fail("Able to open corrupt read-only store (index size = " + indexBytes
                     + ", data bytes = " + dataBytes + ").");
        } catch(VoldemortException e) {
            if(shouldWork)
                fail("Unexpected failure:" + e.getMessage());
        }
    }

    @Test
    public void testSwap() throws IOException {
        createStoreFiles(dir, ReadOnlyUtils.INDEX_ENTRY_SIZE * 5, 4 * 5 * 10, 2);
        ReadOnlyStorageEngine engine = new ReadOnlyStorageEngine("test", strategy, dir, 2);
        assertVersionsExist(dir, 0);

        // swap to a new version
        File newDir = TestUtils.createTempDir();
        createStoreFiles(newDir, 0, 0, 2);
        engine.swapFiles(newDir.getAbsolutePath());
        assertVersionsExist(dir, 0, 1);

        engine.rollback();
        assertVersionsExist(dir, 0);
    }

    @Test(expected = VoldemortException.class)
    public void testBadSwapThrows() throws IOException {
        createStoreFiles(dir, ReadOnlyUtils.INDEX_ENTRY_SIZE * 5, 4 * 5 * 10, 2);
        ReadOnlyStorageEngine engine = new ReadOnlyStorageEngine("test", strategy, dir, 2);
        assertVersionsExist(dir, 0);

        // swap to a new bad version
        File newDir = TestUtils.createTempDir();
        createStoreFiles(newDir, 73, 1024, 2);
        engine.swapFiles(newDir.getAbsolutePath());
    }

    private void assertVersionsExist(File dir, int... versions) {
        for(int i = 0; i < versions.length; i++) {
            File versionDir = new File(dir, "version-" + versions[i]);
            assertTrue("Could not find " + dir + "/version-" + versions[i], versionDir.exists());
        }
        // now check that the next higher version does not exist
        File versionDir = new File(dir, "version-" + versions.length);
        assertFalse("Found version directory that should not exist.", versionDir.exists());
    }

    private void createStoreFiles(File dir, int indexBytes, int dataBytes, int chunks)
            throws IOException, FileNotFoundException {
        for(int chunk = 0; chunk < chunks; chunk++) {
            File index = createFile(dir, chunk + ".index");
            File data = createFile(dir, chunk + ".data");
            // write some random crap for index and data
            FileOutputStream dataOs = new FileOutputStream(data);
            for(int i = 0; i < dataBytes; i++)
                dataOs.write(i);
            dataOs.close();
            FileOutputStream indexOs = new FileOutputStream(index);
            for(int i = 0; i < indexBytes; i++)
                indexOs.write(i);
            indexOs.close();
        }
    }

    private File createFile(File dir, String name) throws IOException {
        dir.mkdirs();
        File data = new File(dir, name);
        data.createNewFile();
        return data;
    }

}
