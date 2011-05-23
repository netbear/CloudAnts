package voldemort.store.parser;

import java.util.ArrayList;

import voldemort.utils.ByteArray;
import voldemort.versioning.Occured;
import voldemort.versioning.VectorClock;

public class AccessNode {

    private AccessType op;
    private String client;
    private ByteArray key;
    private VectorClock version;
    private ArrayList<AccessNode> successors;
    private boolean dirty;
    private long timestamp;

    public AccessNode(AccessType op,
                      String client,
                      ByteArray key,
                      VectorClock version,
                      long timestamp) {
        this.op = op;
        this.client = client;
        this.key = key;
        this.version = version;
        this.timestamp = timestamp;
        this.dirty = false;
    }

    public boolean addSuccessor(AccessNode node) {
        if(!key.equals(node.key))
            return false;

        if(op == AccessType.SET && node.op == AccessType.GET) {
            if(version.compare(node.version) == Occured.BEFORE) {
                successors.add(node);
            }
        }

        return false;
    }

    public void setDirty(boolean d) {
        dirty = d;
    }

    public boolean isDirty() {
        return dirty;
    }

    public String getClient() {
        return client;
    }

    public AccessType getOpType() {
        return op;
    }
}
