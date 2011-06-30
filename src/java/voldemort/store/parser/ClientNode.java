package voldemort.store.parser;

import java.util.ArrayList;

import voldemort.utils.ByteUtils;
import voldemort.versioning.Occured;
import voldemort.versioning.VectorClock;

public class ClientNode {

    private String client;
    private VectorClock version;
    private ArrayList<AccessNode> accesses;

    public ClientNode(String client) {
        this.client = client;
        version = null;
        accesses = new ArrayList<AccessNode>();
    }

    public ClientNode(String client, VectorClock version) {
        this.client = client;
        this.version = version;
        accesses = new ArrayList<AccessNode>();
    }

    public ClientNode(byte[] bytes, int offset) {
        int length = bytes[offset];
        byte[] clientBytes = ByteUtils.copy(bytes, offset + 1, offset + length + 1);
        String c = ByteUtils.getString(clientBytes, "UTF-8");
        this.client = c;
        if(bytes[offset + length + 1] == 1)
            this.version = new VectorClock(bytes, offset + 2 + length);
        else
            this.version = null;

    }

    public String getClient() {
        return client;
    }

    public VectorClock getVersion() {
        return version;
    }

    public int sizeInBytes() {
        return client.length() + 2 + (version == null ? 0 : version.sizeInBytes());
    }

    public byte[] toBytes() {
        byte[] clientBytes = ByteUtils.getBytes(client, "UTF-8");

        byte[] serialized = ByteUtils.cat(new byte[] { (byte) client.length() },
                                          clientBytes,
                                          new byte[] { (byte) (version == null ? 0 : 1) },
                                          version == null ? new byte[0] : version.toBytes());
        return serialized;
    }

    public void addAccess(AccessNode node) {
        if(client.equals(node.getClient()) && node.getOpType() == AccessType.SET) {
            accesses.add(node);
        }
    }

    public boolean setVersion(VectorClock v) {
        boolean isUpdated = true;
        if(version == null) {
            version = v;
            if(v == null)
                isUpdated = false;
        } else {

            VectorClock newVersion = VectorClock.forwardMerge(version, v);

            if(version.getEntries().size() == newVersion.getEntries().size()) {
                isUpdated = false;
                for(int k = 0; k < newVersion.getEntries().size(); k++) {
                    if(!version.getEntries().get(k).equals(newVersion.getEntries().get(k))) {
                        // System.out.println("different version " +
                        // version.getEntries().get(k) + " "
                        // + newVersion.getEntries().get(k));
                        isUpdated = true;
                        break;
                    }
                }
            }

            version = newVersion;
        }

        for(AccessNode node: accesses) {
            if(node.getVersion().compare(version) == Occured.AFTER) {
                node.setDirty();
            }
        }

        return isUpdated;
    }

    @Override
    public String toString() {
        return client + " " + version;
    }

    public void accept(GraphVisitor visitor) {
        visitor.visit(this);
    }
}
