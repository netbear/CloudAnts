package voldemort.store.analyzer;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import voldemort.client.ClientConfig;
import voldemort.client.protocol.RequestFormatType;
import voldemort.store.parser.AccessNode;
import voldemort.store.parser.ClientNode;
import voldemort.store.parser.TrackLogParser;
import voldemort.store.socket.SocketAndStreams;
import voldemort.store.socket.SocketDestination;
import voldemort.store.socket.SocketPool;
import voldemort.utils.ByteUtils;
import voldemort.versioning.VectorClock;

import com.google.common.collect.Maps;

public class AnalyzerMaster {

    private final Logger logger;

    private TrackLogParser parser;
    private final SocketPool socketPool;
    private List<SocketDestination> destList;
    private Map<String, ClientNode> clientsMap;

    public AnalyzerMaster(ClientConfig config, TrackLogParser parser) {
        this.logger = Logger.getLogger(AnalyzerMaster.class.getName());
        socketPool = new SocketPool(config.getMaxConnectionsPerNode(),
                                    config.getConnectionTimeout(TimeUnit.MILLISECONDS),
                                    config.getSocketTimeout(TimeUnit.MILLISECONDS),
                                    config.getSocketBufferSize(),
                                    config.getSocketKeepAlive());
        destList = new ArrayList<SocketDestination>();
        this.parser = parser;
    }

    public void addDestination(String host, int port) {
        SocketDestination dest = new SocketDestination(host, port, RequestFormatType.VOLDEMORT_V2);
        destList.add(dest);
    }

    public void init() throws IOException {
        parser.init();

        Map<String, ClientNode> clients = parser.getClientNode();
        logger.info(parser.getLastVersion());
        if(clients.containsKey(parser.getClient())) {
            clients.get(parser.getClient()).setVersion(parser.getLastVersion());
        }

        for(SocketDestination dest: destList) {
            SocketAndStreams sas = socketPool.checkout(dest);
            byte[] cmdBytes = ByteUtils.getBytes("init", "UTF-8");
            sas.getOutputStream().writeByte(cmdBytes.length);
            sas.getOutputStream().write(cmdBytes);
            sas.getOutputStream().flush();
            int length = sas.getInputStream().readByte();
            byte[] respBytes = new byte[length];
            ByteUtils.read(sas.getInputStream(), respBytes);
            String resp = ByteUtils.getString(respBytes, "UTF-8");
            if(resp.equals("done")) {
                logger.info(dest + "done");
            }
        }

        clientsMap = Maps.newHashMap();
        for(String key: clients.keySet()) {
            clientsMap.put(key, clients.get(key));
        }
        updateClientHistory();

    }

    public boolean updateClientHistory() throws IOException {
        boolean needIter = false;

        for(SocketDestination dest: destList) {
            SocketAndStreams sas = socketPool.checkout(dest);
            byte[] cmdBytes = ByteUtils.getBytes("update", "UTF-8");
            sas.getOutputStream().writeByte(cmdBytes.length);
            sas.getOutputStream().write(cmdBytes);
            sas.getOutputStream().flush();

            byte[] clientBytes = getClientBytes();
            sas.getOutputStream().writeInt(clientBytes.length);
            sas.getOutputStream().write(clientBytes);
            sas.getOutputStream().flush();

            int isUpdated = sas.getInputStream().readByte();
            if(isUpdated == 1) {
                logger.info("update from client");
                needIter = true;
                List<ClientNode> slaveClients = getClientFromBytes(sas.getInputStream());
                updateClientsMap(slaveClients);
            }

            int length = sas.getInputStream().readByte();
            byte[] respBytes = new byte[length];
            ByteUtils.read(sas.getInputStream(), respBytes);
            String resp = ByteUtils.getString(respBytes, "UTF-8");
            if(resp.equals("done")) {
                logger.info(dest + "done");
            }
        }

        Map<String, ClientNode> clients = parser.getClientNode();

        boolean nextIter = false;
        if(needIter)
            for(ClientNode client: clientsMap.values()) {
                logger.info("update client: " + client);
                if(clients.containsKey(client.getClient())) {
                    logger.info("current version:" + clients.get(client.getClient()));
                    if(clients.get(client.getClient()).setVersion(client.getVersion())) {
                        nextIter = true;
                        logger.info("update!");
                    }
                }
            }
        if(nextIter)
            logger.info("need more iterations");
        return nextIter;
    }

    public void updateClientsMap(List<ClientNode> clients) {
        for(ClientNode c: clients) {
            if(clientsMap.containsKey(c.getClient())) {
                VectorClock newVersion = VectorClock.forwardMerge(c.getVersion(),
                                                                  clientsMap.get(c.getClient())
                                                                            .getVersion());
                clientsMap.put(c.getClient(), new ClientNode(c.getClient(), newVersion));

            } else {
                clientsMap.put(c.getClient(), c);
            }
        }
    }

    public byte[] getClientBytes() {
        Collection<ClientNode> clients = parser.getClientNode().values();
        int length = 0;
        for(ClientNode c: clients) {
            logger.info(c + " " + c.sizeInBytes() + " bytes.");
            length += c.sizeInBytes();
        }
        byte[] serialized = new byte[length];

        int pos = 0;
        for(ClientNode c: clients) {
            System.arraycopy(c.toBytes(), 0, serialized, pos, c.sizeInBytes());
            pos += c.sizeInBytes();
        }
        return serialized;
    }

    public List<ClientNode> getClientFromBytes(DataInputStream in) throws IOException {
        int valueSize = in.readInt();
        byte[] valueBytes = new byte[valueSize];
        ByteUtils.read(in, valueBytes);
        int offset = 0;
        // logger.info("reading " + valueSize + " bytes.");
        ArrayList<ClientNode> clientList = new ArrayList<ClientNode>();
        while(offset < valueSize) {
            ClientNode client = new ClientNode(valueBytes, offset);
            // logger.info(client + " " + client.sizeInBytes() + " bytes.");
            offset += client.sizeInBytes();
            clientList.add(client);
        }

        return clientList;
    }

    public void outputDirtyKeys() {
        Map<String, ClientNode> clients = parser.getClientNode();

        logger.info("dirty keys");
        for(ClientNode client: clients.values()) {
            ArrayList<AccessNode> nodeList = client.getClientOp();
            for(AccessNode node: nodeList) {
                if(node.isDirty()) {
                    String key = ByteUtils.getString(node.getKey().get(), "UTF-8");
                    logger.info(node.getClient() + " : " + key);
                } else {
                    String key = ByteUtils.getString(node.getKey().get(), "UTF-8");
                    logger.info(node.getClient() + " not dirty : " + key);
                }
            }
        }
    }
}
