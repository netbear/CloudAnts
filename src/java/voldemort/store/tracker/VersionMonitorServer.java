package voldemort.store.tracker;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.ClientConfig;
import voldemort.client.protocol.RequestFormatType;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.socket.SocketAndStreams;
import voldemort.store.socket.SocketDestination;
import voldemort.store.socket.SocketPool;
import voldemort.utils.ByteUtils;
import voldemort.versioning.ClockEntry;
import voldemort.versioning.VectorClock;

import com.google.common.collect.Maps;

public class VersionMonitorServer extends Thread {

    private final static String LOCAL_CLOCK = ".localclock";
    private AtomicLong localClock;
    private Map<Integer, Long> clusterClock;
    private final ThreadPoolExecutor threadPool;
    private final Logger logger;
    private ServerSocket serverSocket = null;
    private int port, socketBufferSize, localNode;
    private MetadataStore metaStore;
    private List<SocketDestination> destList;
    private ExecutorService requestPool;
    private final SocketPool socketPool;
    private final Timer timer;
    private static VersionMonitorServer serverInstance = null;
    private Object clusterClockLock = new Object();
    private Object localClockLock = new Object();
    private AtomicBoolean isInit = new AtomicBoolean(false);

    // private static final ThreadLocal<String> clientAddr = new
    // ThreadLocal<String>();

    public static VersionMonitorServer getServerInstance() {
        if(serverInstance != null)
            return serverInstance;
        else
            return null;
    }

    public static VersionMonitorServer getServerInstance(MetadataStore metaStore,
                                                         int port,
                                                         int defaultThreads,
                                                         int maxThreads,
                                                         int socketBufferSize) {
        if(serverInstance == null)
            serverInstance = new VersionMonitorServer(metaStore,
                                                      port,
                                                      defaultThreads,
                                                      maxThreads,
                                                      socketBufferSize);
        return serverInstance;
    }

    private VersionMonitorServer(MetadataStore metaStore,
                                 int port,
                                 int defaultThreads,
                                 int maxThreads,
                                 int socketBufferSize) {
        this.logger = Logger.getLogger(VersionMonitorServer.class.getName());
        this.metaStore = metaStore;
        File f = new File(LOCAL_CLOCK);
        localClock = new AtomicLong(0);
        try {
            if(f.exists()) {
                BufferedReader reader = new BufferedReader(new FileReader(f));
                localClock.set(Long.parseLong(reader.readLine()));
                reader.close();
            } else
                localClock.set(1);
        } catch(IOException ex) {}

        this.clusterClock = Maps.newLinkedHashMap();
        this.port = port;
        this.localNode = metaStore.getNodeId();
        this.socketBufferSize = socketBufferSize;

        this.threadPool = new ThreadPoolExecutor(defaultThreads,
                                                 maxThreads,
                                                 0,
                                                 TimeUnit.MILLISECONDS,
                                                 new SynchronousQueue<Runnable>());

        Cluster cluster = this.metaStore.getCluster();
        destList = new ArrayList<SocketDestination>();
        for(Node n: cluster.getNodes())
            if(n.getId() != metaStore.getNodeId()) {
                String host = n.getHost();
                int p = port;
                SocketDestination dest = new SocketDestination(host,
                                                               p,
                                                               RequestFormatType.VOLDEMORT_V2);
                destList.add(dest);
            }
        this.requestPool = Executors.newFixedThreadPool(cluster.getNodes().size() + 1);
        ClientConfig config = new ClientConfig();
        this.socketPool = new SocketPool(config.getMaxConnectionsPerNode(),
                                         config.getConnectionTimeout(TimeUnit.MILLISECONDS),
                                         config.getSocketTimeout(TimeUnit.MILLISECONDS),
                                         config.getSocketBufferSize(),
                                         config.getSocketKeepAlive());

        TimerTask scheduleTask = new TimerTask() {

            @Override
            public void run() {
                writeBackClock();
                // updateClusterClock();
            }
        };

        timer = new Timer();
        timer.schedule(scheduleTask, 1000, 1000);
    }

    private void writeBackClock() {
        synchronized(localClockLock) {
            File f = new File(LOCAL_CLOCK);
            try {
                BufferedWriter writer = new BufferedWriter(new FileWriter(f));
                writer.write(Long.toString(localClock.get()));
                writer.close();
            } catch(IOException ex) {}
        }
    }

    public void shutdown() {
        logger.info("Shutting down CloudAnts VersionMonitor");

        interrupt();

        try {
            if(!serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch(IOException e) {
            logger.error("Error while closing socket server: " + e.getMessage());
        } finally {
            writeBackClock();
        }
    }

    public long getLocalClock() {
        return localClock.get();
    }

    public long incLocalClock() {
        return localClock.incrementAndGet();
    }

    public void setClusterClock(int node, long clock) {
        synchronized(clusterClockLock) {
            // logger.info("Set " + node + " " + clock);
            clusterClock.put(node, clock);
        }
    }

    public List<ClockEntry> getClusterClock() {
        List<ClockEntry> versions = new ArrayList<ClockEntry>();

        synchronized(clusterClockLock) {
            Iterator<Entry<Integer, Long>> iter = clusterClock.entrySet().iterator();
            while(iter.hasNext()) {
                Entry<Integer, Long> entry = iter.next();
                versions.add(new ClockEntry(entry.getKey().shortValue(), entry.getValue()));

            }
        }

        return versions;
    }

    public int getNode() {
        return localNode;
    }

    private class IncomingRequestHandler implements Runnable {

        private VersionMonitorServer server;
        private Socket socket;

        public IncomingRequestHandler(VersionMonitorServer server, Socket socket) {
            this.server = server;
            this.socket = socket;
        }

        public void run() {
            try {
                DataInputStream inputStream = new DataInputStream(new BufferedInputStream(socket.getInputStream(),
                                                                                          64000));

                DataOutputStream outputStream = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(),
                                                                                              64000));

                // logger.info("nego protocol");
                negotiateProtocol(inputStream, outputStream);
                while(true) {
                    int node = inputStream.readInt();
                    long clock = inputStream.readLong();
                    server.setClusterClock(node, clock);

                    // logger.info("sending local clock");
                    outputStream.writeInt(localNode);
                    outputStream.writeLong(server.getLocalClock());
                    outputStream.flush();
                }

            } catch(EOFException ex) {
                logger.info("Ending Incoming Request Handler");
            } catch(IOException ex) {
                logger.info(ex);
            } finally {
                try {
                    socket.close();
                } catch(IOException e) {
                    logger.warn("Error while shutting down incoming request socket.", e);
                }
            }
        }

        private RequestFormatType negotiateProtocol(InputStream input, OutputStream output)
                throws IOException {
            input.mark(3);
            byte[] protoBytes = new byte[3];
            ByteUtils.read(input, protoBytes);
            RequestFormatType requestFormat;
            try {
                String proto = ByteUtils.getString(protoBytes, "UTF-8");
                requestFormat = RequestFormatType.fromCode(proto);
                output.write(ByteUtils.getBytes("ok", "UTF-8"));
                output.flush();
            } catch(IllegalArgumentException e) {
                // okay we got some nonsense. For backwards compatibility,
                // assume this is an old client who does not know how to
                // negotiate
                requestFormat = RequestFormatType.VOLDEMORT_V0;
                // reset input stream so we don't interfere with request format
                input.reset();
                logger.info("No protocol proposal given, assuming "
                            + RequestFormatType.VOLDEMORT_V0.getDisplayName());
            }
            return requestFormat;
        }
    }

    private class OutgoingRequestHandler implements Runnable {

        private final SocketDestination dest;
        private final VersionMonitorServer server;

        public OutgoingRequestHandler(VersionMonitorServer server, SocketDestination dest) {
            this.dest = dest;
            this.server = server;
        }

        public void run() {
            SocketAndStreams sas = socketPool.checkout(dest);

            try {
                // logger.info("Outgoing : write");
                sas.getOutputStream().writeInt(server.getNode());
                sas.getOutputStream().writeLong(server.getLocalClock());
                // logger.info("Outgoing : flush");
                sas.getOutputStream().flush();

                // logger.info("Outgoing : reading cluster version");
                int node = sas.getInputStream().readInt();
                long clock = sas.getInputStream().readLong();
                // logger.info("Outgoing : set Cluster clock");
                server.setClusterClock(node, clock);

            } catch(IOException e) {
                logger.error(e);
            }

            socketPool.checkin(dest, sas);
            // logger.info("OutgoinRequest handler end!");
        }
    }

    @Override
    public void run() {
        logger.info("Starting CloudAnts version monitor on port " + port);
        try {
            serverSocket = new ServerSocket();
            serverSocket.bind(new InetSocketAddress(port));
            serverSocket.setReceiveBufferSize(this.socketBufferSize);

            while(!isInterrupted() && !serverSocket.isClosed()) {
                final Socket socket = serverSocket.accept();
                socket.setTcpNoDelay(true);
                socket.setSendBufferSize(this.socketBufferSize);

                this.threadPool.execute(new IncomingRequestHandler(this, socket));
            }
        } catch(BindException e) {
            logger.error("Could not bind to port " + port + ".");
            throw new VoldemortException(e);
        } catch(SocketException e) {
            // If we have been manually shutdown, ignore
            if(!isInterrupted())
                logger.error("Error in server: ", e);
        } catch(IOException e) {
            logger.info("VersionMonitor Server :" + e.toString());
            throw new VoldemortException(e);
        } catch(Throwable t) {
            logger.error(t);
            if(t instanceof Error)
                throw (Error) t;
            else if(t instanceof RuntimeException)
                throw (RuntimeException) t;
            throw new VoldemortException(t);
        } finally {
            // logger.info("Closing server");
            if(serverSocket != null) {
                try {
                    serverSocket.close();
                } catch(IOException e) {
                    logger.warn("Error while shutting down server.", e);
                }
            }

        }
    }

    public synchronized void updateClusterClock() {
        // logger.info("udpate cluster clock");
        ArrayList<Future<?>> futures = new ArrayList<Future<?>>();
        for(SocketDestination dest: destList) {
            // logger.info("Dest : " + dest);
            futures.add(requestPool.submit(new OutgoingRequestHandler(this, dest)));
        }

        for(Future<?> f: futures) {
            try {
                f.get();
            } catch(InterruptedException e) {
                e.printStackTrace();
            } catch(ExecutionException e) {
                e.printStackTrace();
            }
        }
        // logger.info("ending update cluster clock");
    }

    public VectorClock getClusterVersion() {
        return getClusterVersion(localClock.get());
    }

    public VectorClock getClusterVersion(long clock) {
        // logger.info("Get cluster version");
        Object initLock = new Object();
        if(isInit.get() == false) {
            synchronized(initLock) {
                if(isInit.get() == false)
                    updateClusterClock();
                else
                    logger.info("already init");
                isInit.set(true);
            }
        }

        List<ClockEntry> versions = this.getClusterClock();

        versions.add(new ClockEntry((short) localNode, clock));
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

        return new VectorClock(versions, System.currentTimeMillis());
    }

}
