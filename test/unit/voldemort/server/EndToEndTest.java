package voldemort.server;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.versioning.Versioned;

import static org.junit.runners.Parameterized.Parameters;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.*;

/**
 * Provides an unmocked end to end unit test of a Voldemort cluster.
 *
 */
@RunWith(Parameterized.class)
public class EndToEndTest {

    private static final String STORE_NAME = "test-readrepair-memory";
    private static final String STORES_XML = "test/common/voldemort/config/stores.xml";

    private final boolean useNio;

    private List<VoldemortServer> servers;
    private Cluster cluster;
    private StoreClient<String,String> storeClient;

    public EndToEndTest(boolean useNio) {
        this.useNio = useNio;
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { true }, { false } });
    }

    @Before
    public void setUp() throws IOException {
        cluster = ServerTestUtils.getLocalCluster(2, new int[][] { { 0, 2, 4, 6 }, { 1, 3, 5, 7 } });
        servers = new ArrayList<VoldemortServer>();
        servers.add(ServerTestUtils.startVoldemortServer(ServerTestUtils.createServerConfig(useNio,
                                                                                            0,
                                                                                            TestUtils.createTempDir()
                                                                                                    .getAbsolutePath(),
                                                                                            null,
                                                                                            STORES_XML,
                                                                                            new Properties()),
                                                         cluster));
        servers.add(ServerTestUtils.startVoldemortServer(ServerTestUtils.createServerConfig(useNio,
                                                                                            1,
                                                                                            TestUtils.createTempDir()
                                                                                                    .getAbsolutePath(),
                                                                                            null,
                                                                                            STORES_XML,
                                                                                            new Properties()),
                                                         cluster));
        Node node = cluster.getNodeById(0);
        String bootstrapUrl = "tcp://" + node.getHost() + ":" + node.getSocketPort();
        StoreClientFactory storeClientFactory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrapUrl));
        storeClient = storeClientFactory.getStoreClient(STORE_NAME);
    }

    /**
     * Test the basic get/getAll/put/delete functionality.
     */
    @Test
    public void testSanity() {
        storeClient.put("Belarus", "Minsk");
        storeClient.put("Russia", "Moscow");
        storeClient.put("Ukraine", "Kiev");
        storeClient.put("Kazakhstan", "Almaty");

        Versioned<String> v1 = storeClient.get("Belarus");
        assertEquals("get/put work as expected", "Minsk", v1.getValue());

        storeClient.put("Kazakhstan", "Astana");
        Versioned<String> v2 = storeClient.get("Kazakhstan");
        assertEquals("clobbering a value works as expected, we have read-your-writes consistency", "Astana", v2.getValue());

        Map<String, Versioned<String>> capitals = storeClient.getAll(Arrays.asList("Russia", "Ukraine"));

        assertEquals("getAll works as expected", "Moscow", capitals.get("Russia").getValue());
        assertEquals("getAll works as expected", "Kiev", capitals.get("Ukraine").getValue());

        storeClient.delete("Ukraine");
        assertNull("delete works as expected", storeClient.get("Ukraine"));
    }
}
