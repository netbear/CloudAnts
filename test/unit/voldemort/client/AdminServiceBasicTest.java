/*
 * Copyright 2008-2009 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.rebalance.RebalancePartitionsInfo;
import voldemort.cluster.Cluster;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyType;
import voldemort.serialization.SerializerDefinition;
import voldemort.server.VoldemortServer;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.memory.InMemoryStorageConfiguration;
import voldemort.store.metadata.MetadataStore;
import voldemort.utils.ByteArray;
import voldemort.utils.Pair;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

import com.google.common.collect.AbstractIterator;

/**
 */
@RunWith(Parameterized.class)
public class AdminServiceBasicTest extends TestCase {

    private static int NUM_RUNS = 100;
    private static int TEST_STREAM_KEYS_SIZE = 10000;
    private static String testStoreName = "test-replication-memory";
    private static String storesXmlfile = "test/common/voldemort/config/stores.xml";

    private VoldemortServer[] servers;
    private Cluster cluster;
    private AdminClient adminClient;

    private final boolean useNio;

    public AdminServiceBasicTest(boolean useNio) {
        this.useNio = useNio;
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { true }, { false } });
    }

    @Override
    @Before
    public void setUp() throws IOException {
        cluster = ServerTestUtils.getLocalCluster(2, new int[][] { { 0, 1, 2, 3 }, { 4, 5, 6, 7 } });
        servers = new VoldemortServer[2];

        servers[0] = ServerTestUtils.startVoldemortServer(ServerTestUtils.createServerConfig(useNio,
                                                                                             0,
                                                                                             TestUtils.createTempDir()
                                                                                                      .getAbsolutePath(),
                                                                                             null,
                                                                                             storesXmlfile,
                                                                                             new Properties()),
                                                          cluster);
        servers[1] = ServerTestUtils.startVoldemortServer(ServerTestUtils.createServerConfig(useNio,
                                                                                             1,
                                                                                             TestUtils.createTempDir()
                                                                                                      .getAbsolutePath(),
                                                                                             null,
                                                                                             storesXmlfile,
                                                                                             new Properties()),
                                                          cluster);

        adminClient = ServerTestUtils.getAdminClient(cluster);
    }

    @Override
    @After
    public void tearDown() throws IOException, InterruptedException {
        adminClient.stop();
        for(VoldemortServer server: servers) {
            ServerTestUtils.stopVoldemortServer(server);
        }
    }

    private VoldemortServer getVoldemortServer(int nodeId) {
        return servers[nodeId];
    }

    private AdminClient getAdminClient() {
        return adminClient;
    }

    private Store<ByteArray, byte[]> getStore(int nodeID, String storeName) {
        Store<ByteArray, byte[]> store = getVoldemortServer(nodeID).getStoreRepository()
                                                                   .getStorageEngine(storeName);
        assertNotSame("Store '" + storeName + "' should not be null", null, store);
        return store;
    }

    private boolean isKeyPartition(ByteArray key,
                                   int nodeId,
                                   String storeName,
                                   List<Integer> deletePartitionsList) {
        RoutingStrategy routing = getVoldemortServer(nodeId).getMetadataStore()
                                                            .getRoutingStrategy(storeName);
        for(int partition: routing.getPartitionList(key.get())) {
            if(deletePartitionsList.contains(partition)) {
                return true;
            }
        }
        return false;
    }

    @Test
    public void testUpdateClusterMetadata() {
        Cluster updatedCluster = ServerTestUtils.getLocalCluster(4);
        AdminClient client = getAdminClient();
        for(int i = 0; i < NUM_RUNS; i++) {
            VectorClock clock = ((VectorClock) client.getRemoteCluster(0).getVersion()).incremented(0,
                                                                                                    System.currentTimeMillis());
            client.updateRemoteCluster(0, updatedCluster, clock);

            assertEquals("Cluster should match",
                         updatedCluster,
                         getVoldemortServer(0).getMetadataStore().getCluster());
            assertEquals("AdminClient.getMetdata() should match", client.getRemoteCluster(0)
                                                                        .getValue(), updatedCluster);

            // version should match
            assertEquals("versions should match as well.", clock, client.getRemoteCluster(0)
                                                                        .getVersion());
        }

    }

    @Test
    public void testAddStore() throws Exception {
        AdminClient adminClient = getAdminClient();

        StoreDefinition definition = new StoreDefinitionBuilder().setName("updateTest")
                                                                 .setType(InMemoryStorageConfiguration.TYPE_NAME)
                                                                 .setKeySerializer(new SerializerDefinition("string"))
                                                                 .setValueSerializer(new SerializerDefinition("string"))
                                                                 .setRoutingPolicy(RoutingTier.CLIENT)
                                                                 .setRoutingStrategyType(RoutingStrategyType.CONSISTENT_STRATEGY)
                                                                 .setReplicationFactor(1)
                                                                 .setPreferredReads(1)
                                                                 .setRequiredReads(1)
                                                                 .setPreferredWrites(1)
                                                                 .setRequiredWrites(1)
                                                                 .build();
        adminClient.addStore(definition);

        // now test the store
        StoreClientFactory factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(cluster.getNodeById(0)
                                                                                                             .getSocketUrl()
                                                                                                             .toString()));

        StoreClient<Object, Object> client = factory.getStoreClient("updateTest");
        client.put("abc", "123");
        String s = (String) client.get("abc").getValue();
        assertEquals(s, "123");

        // test again with a unknown store
        try {
            client = factory.getStoreClient("updateTest2");
            client.put("abc", "123");
            s = (String) client.get("abc").getValue();
            assertEquals(s, "123");
            fail("Should have received bootstrap failure exception");
        } catch(Exception e) {
            if(!(e instanceof BootstrapFailureException))
                throw e;
        }

        // make sure that the store list we get back from AdminClient
        Versioned<List<StoreDefinition>> list = adminClient.getRemoteStoreDefList(0);
        assertTrue(list.getValue().contains(definition));
    }

    @Test
    public void testStateTransitions() {
        // change to REBALANCING STATE
        AdminClient client = getAdminClient();
        client.updateRemoteServerState(getVoldemortServer(0).getIdentityNode().getId(),
                                       MetadataStore.VoldemortState.REBALANCING_MASTER_SERVER,
                                       ((VectorClock) client.getRemoteServerState(0).getVersion()).incremented(0,
                                                                                                               System.currentTimeMillis()));

        MetadataStore.VoldemortState state = getVoldemortServer(0).getMetadataStore()
                                                                  .getServerState();
        assertEquals("State should be changed correctly to rebalancing state",
                     MetadataStore.VoldemortState.REBALANCING_MASTER_SERVER,
                     state);

        // change back to NORMAL state
        client.updateRemoteServerState(getVoldemortServer(0).getIdentityNode().getId(),
                                       MetadataStore.VoldemortState.NORMAL_SERVER,
                                       ((VectorClock) client.getRemoteServerState(0).getVersion()).incremented(0,
                                                                                                               System.currentTimeMillis()));

        state = getVoldemortServer(0).getMetadataStore().getServerState();
        assertEquals("State should be changed correctly to rebalancing state",
                     MetadataStore.VoldemortState.NORMAL_SERVER,
                     state);

        // lets revert back to REBALANCING STATE AND CHECK
        client.updateRemoteServerState(getVoldemortServer(0).getIdentityNode().getId(),
                                       MetadataStore.VoldemortState.REBALANCING_MASTER_SERVER,
                                       ((VectorClock) client.getRemoteServerState(0).getVersion()).incremented(0,
                                                                                                               System.currentTimeMillis()));

        state = getVoldemortServer(0).getMetadataStore().getServerState();

        assertEquals("State should be changed correctly to rebalancing state",
                     MetadataStore.VoldemortState.REBALANCING_MASTER_SERVER,
                     state);

        client.updateRemoteServerState(getVoldemortServer(0).getIdentityNode().getId(),
                                       MetadataStore.VoldemortState.NORMAL_SERVER,
                                       ((VectorClock) client.getRemoteServerState(0).getVersion()).incremented(0,
                                                                                                               System.currentTimeMillis()));

        state = getVoldemortServer(0).getMetadataStore().getServerState();
        assertEquals("State should be changed correctly to rebalancing state",
                     MetadataStore.VoldemortState.NORMAL_SERVER,
                     state);
    }

    @Test
    public void testDeletePartitionEntries() {
        HashMap<ByteArray, byte[]> entrySet = ServerTestUtils.createRandomKeyValuePairs(TEST_STREAM_KEYS_SIZE);

        // insert it into server-0 store
        Store<ByteArray, byte[]> store = getStore(0, testStoreName);
        for(Entry<ByteArray, byte[]> entry: entrySet.entrySet()) {
            store.put(entry.getKey(), new Versioned<byte[]>(entry.getValue()));
        }

        List<Integer> deletePartitionsList = Arrays.asList(0, 2);

        // do delete partitions request
        getAdminClient().deletePartitions(0, testStoreName, deletePartitionsList, null);

        store = getStore(0, testStoreName);
        for(Entry<ByteArray, byte[]> entry: entrySet.entrySet()) {
            if(isKeyPartition(entry.getKey(), 0, testStoreName, deletePartitionsList)) {
                assertEquals("deleted partitions should be missing.", 0, store.get(entry.getKey())
                                                                              .size());
            }
        }
    }

    @Test
    public void testFetchPartitionKeys() {

        HashMap<ByteArray, byte[]> entrySet = ServerTestUtils.createRandomKeyValuePairs(TEST_STREAM_KEYS_SIZE);
        List<Integer> fetchPartitionsList = Arrays.asList(0, 2);

        // insert it into server-0 store
        int fetchPartitionKeyCount = 0;
        Store<ByteArray, byte[]> store = getStore(0, testStoreName);
        for(Entry<ByteArray, byte[]> entry: entrySet.entrySet()) {
            store.put(entry.getKey(), new Versioned<byte[]>(entry.getValue()));
            if(isKeyPartition(entry.getKey(), 0, testStoreName, fetchPartitionsList)) {
                fetchPartitionKeyCount++;
            }
        }

        Iterator<ByteArray> fetchIt = getAdminClient().fetchKeys(0,
                                                                 testStoreName,
                                                                 fetchPartitionsList,
                                                                 null,
                                                                 false);
        // check values
        int count = 0;
        while(fetchIt.hasNext()) {
            assertEquals("Fetched key should belong to asked partitions",
                         true,
                         isKeyPartition(fetchIt.next(), 0, testStoreName, fetchPartitionsList));
            count++;
        }

        // assert all keys for asked partitions are returned.
        assertEquals("All keys for asked partitions should be received",
                     fetchPartitionKeyCount,
                     count);
    }

    @Test
    public void testTruncate() throws Exception {
        HashMap<ByteArray, byte[]> entrySet = ServerTestUtils.createRandomKeyValuePairs(TEST_STREAM_KEYS_SIZE);

        // insert it into server-0 store
        Store<ByteArray, byte[]> store = getStore(0, testStoreName);
        for(Entry<ByteArray, byte[]> entry: entrySet.entrySet()) {
            store.put(entry.getKey(), new Versioned<byte[]>(entry.getValue()));
        }

        // do truncate request
        getAdminClient().truncate(0, testStoreName);

        store = getStore(0, testStoreName);

        for(Entry<ByteArray, byte[]> entry: entrySet.entrySet()) {
            assertEquals("Deleted key should be missing.", 0, store.get(entry.getKey()).size());
        }
    }

    @Test
    public void testFetch() {
        HashMap<ByteArray, byte[]> entrySet = ServerTestUtils.createRandomKeyValuePairs(TEST_STREAM_KEYS_SIZE);
        List<Integer> fetchPartitionsList = Arrays.asList(0, 2);

        // insert it into server-0 store
        int fetchPartitionKeyCount = 0;
        Store<ByteArray, byte[]> store = getStore(0, testStoreName);
        for(Entry<ByteArray, byte[]> entry: entrySet.entrySet()) {
            store.put(entry.getKey(), new Versioned<byte[]>(entry.getValue()));
            if(isKeyPartition(entry.getKey(), 0, testStoreName, fetchPartitionsList)) {
                fetchPartitionKeyCount++;
            }
        }

        Iterator<Pair<ByteArray, Versioned<byte[]>>> fetchIt = getAdminClient().fetchEntries(0,
                                                                                             testStoreName,
                                                                                             fetchPartitionsList,
                                                                                             null,
                                                                                             false);
        // check values
        int count = 0;
        while(fetchIt.hasNext()) {
            Pair<ByteArray, Versioned<byte[]>> entry = fetchIt.next();
            assertEquals("Fetched entries should belong to asked partitions",
                         true,
                         isKeyPartition(entry.getFirst(), 0, testStoreName, fetchPartitionsList));
            assertEquals("entry value should match",
                         new String(entry.getSecond().getValue()),
                         new String(entrySet.get(entry.getFirst())));
            count++;
        }

        // assert all keys for asked partitions are returned.
        assertEquals("All entries for asked partitions should be received",
                     fetchPartitionKeyCount,
                     count);

    }

    @Test
    public void testUpdate() {
        final HashMap<ByteArray, byte[]> entrySet = ServerTestUtils.createRandomKeyValuePairs(TEST_STREAM_KEYS_SIZE);

        Iterator<Pair<ByteArray, Versioned<byte[]>>> iterator = new AbstractIterator<Pair<ByteArray, Versioned<byte[]>>>() {

            final Iterator<Entry<ByteArray, byte[]>> entrySetItr = entrySet.entrySet().iterator();

            @Override
            protected Pair<ByteArray, Versioned<byte[]>> computeNext() {
                while(entrySetItr.hasNext()) {
                    Entry<ByteArray, byte[]> entry = entrySetItr.next();
                    return new Pair<ByteArray, Versioned<byte[]>>(entry.getKey(),
                                                                  new Versioned<byte[]>(entry.getValue()));
                }
                return endOfData();
            }
        };

        getAdminClient().updateEntries(0, testStoreName, iterator, null);

        // check updated values
        Store<ByteArray, byte[]> store = getStore(0, testStoreName);
        for(Entry<ByteArray, byte[]> entry: entrySet.entrySet()) {
            assertNotSame("entry should be present at store", 0, store.get(entry.getKey()).size());
            assertEquals("entry value should match",
                         new String(entry.getValue()),
                         new String(store.get(entry.getKey()).get(0).getValue()));
        }
    }

    // check the basic rebalanceNode call.
    @Test
    public void testRebalanceNode() {
        HashMap<ByteArray, byte[]> entrySet = ServerTestUtils.createRandomKeyValuePairs(TEST_STREAM_KEYS_SIZE);
        List<Integer> fetchAndUpdatePartitionsList = Arrays.asList(0, 2);

        // insert it into server-0 store
        int fetchPartitionKeyCount = 0;
        Store<ByteArray, byte[]> store = getStore(0, testStoreName);
        for(Entry<ByteArray, byte[]> entry: entrySet.entrySet()) {
            store.put(entry.getKey(), new Versioned<byte[]>(entry.getValue()));
            if(isKeyPartition(entry.getKey(), 0, testStoreName, fetchAndUpdatePartitionsList)) {
                fetchPartitionKeyCount++;
            }
        }

        List<Integer> rebalancePartitionList = Arrays.asList(1, 3);
        RebalancePartitionsInfo stealInfo = new RebalancePartitionsInfo(1,
                                                                        0,
                                                                        rebalancePartitionList,
                                                                        new ArrayList<Integer>(0),
                                                                        Arrays.asList(testStoreName),
                                                                        0);
        int asyncId = adminClient.rebalanceNode(stealInfo);
        assertNotSame("Got a valid rebalanceAsyncId", -1, asyncId);

        getAdminClient().waitForCompletion(1, asyncId, 120, TimeUnit.SECONDS);

        // assert data is copied correctly
        store = getStore(1, testStoreName);
        for(Entry<ByteArray, byte[]> entry: entrySet.entrySet()) {
            if(isKeyPartition(entry.getKey(), 1, testStoreName, rebalancePartitionList)) {
                assertSame("entry should be present at store", 1, store.get(entry.getKey()).size());
                assertEquals("entry value should match",
                             new String(entry.getValue()),
                             new String(store.get(entry.getKey()).get(0).getValue()));
            }
        }
    }

    @Test
    public void testRecoverData() {
        // use store with replication 2, required write 2 for this test.
        String testStoreName = "test-recovery-data";

        HashMap<ByteArray, byte[]> entrySet = ServerTestUtils.createRandomKeyValuePairs(TEST_STREAM_KEYS_SIZE);
        // insert it into server-0 store
        Store<ByteArray, byte[]> store = getStore(0, testStoreName);
        for(Entry<ByteArray, byte[]> entry: entrySet.entrySet()) {
            store.put(entry.getKey(), new Versioned<byte[]>(entry.getValue()));
        }

        // assert server 1 is empty
        store = getStore(1, testStoreName);
        for(Entry<ByteArray, byte[]> entry: entrySet.entrySet()) {
            assertSame("entry should NOT be present at store", 0, store.get(entry.getKey()).size());
        }

        // recover all data
        adminClient.restoreDataFromReplications(1, 2);

        // assert server 1 has all entries for its partitions
        store = getStore(1, testStoreName);
        for(Entry<ByteArray, byte[]> entry: entrySet.entrySet()) {
            ByteArray key = entry.getKey();
            assertSame("entry should be present for key " + key, 1, store.get(entry.getKey())
                                                                         .size());
            assertEquals("entry value should match",
                         new String(entry.getValue()),
                         new String(store.get(entry.getKey()).get(0).getValue()));
        }
    }

    /**
     * @throws IOException
     */
    @Test
    public void testFetchAndUpdate() throws IOException {
        HashMap<ByteArray, byte[]> entrySet = ServerTestUtils.createRandomKeyValuePairs(TEST_STREAM_KEYS_SIZE);
        List<Integer> fetchAndUpdatePartitionsList = Arrays.asList(0, 2);

        // insert it into server-0 store
        int fetchPartitionKeyCount = 0;
        Store<ByteArray, byte[]> store = getStore(0, testStoreName);
        for(Entry<ByteArray, byte[]> entry: entrySet.entrySet()) {
            store.put(entry.getKey(), new Versioned<byte[]>(entry.getValue()));
            if(isKeyPartition(entry.getKey(), 0, testStoreName, fetchAndUpdatePartitionsList)) {
                fetchPartitionKeyCount++;
            }
        }

        // assert that server1 is empty.
        store = getStore(1, testStoreName);
        for(Entry<ByteArray, byte[]> entry: entrySet.entrySet())
            assertEquals("server1 should be empty at start.", 0, store.get(entry.getKey()).size());

        // do fetch And update call server1 <-- server0
        AdminClient client = getAdminClient();
        int id = client.migratePartitions(0, 1, testStoreName, fetchAndUpdatePartitionsList, null);
        client.waitForCompletion(1, id, 60, TimeUnit.SECONDS);

        // check values
        int count = 0;
        store = getStore(1, testStoreName);
        for(Entry<ByteArray, byte[]> entry: entrySet.entrySet()) {
            if(isKeyPartition(entry.getKey(), 0, testStoreName, fetchAndUpdatePartitionsList)) {
                assertEquals("server1 store should contain fetchAndupdated partitions.",
                             1,
                             store.get(entry.getKey()).size());
                assertEquals("entry value should match",
                             new String(entry.getValue()),
                             new String(store.get(entry.getKey()).get(0).getValue()));
                count++;
            }
        }

        // assert all keys for asked partitions are returned.
        assertEquals("All keys for asked partitions should be received",
                     fetchPartitionKeyCount,
                     count);

    }
}
