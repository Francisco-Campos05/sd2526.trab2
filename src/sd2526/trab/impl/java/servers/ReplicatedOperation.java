package sd2526.trab.impl.java.servers;

import java.util.List;
import java.util.Set;

import sd2526.trab.api.Message;

public class ReplicatedOperation {

    public enum Type { POST_MESSAGE, DELETE_MESSAGE, REMOTE_POST, REMOTE_DELETE, DELETE_INBOX, REMOVE_INBOX_ENTRY }

    private Type type;
    private long seqNum;
    private Long sid;
    private String sourceDomain;

    private Message message;
    private List<String> localRecipients;
    private Set<String> remoteDestinations;

    private String messageId;
    private Set<String> destinations;

    private String userName;

    /** URI of the replica that published this op — used to restrict cross-domain dispatch to one replica. */
    private String publisherId;

    public ReplicatedOperation() {}

    public static ReplicatedOperation postMessage(long seqNum, Message message,
            List<String> localRecipients, Set<String> remoteDestinations) {
        var op = new ReplicatedOperation();
        op.type = Type.POST_MESSAGE;
        op.seqNum = seqNum;
        op.message = message;
        op.localRecipients = localRecipients;
        op.remoteDestinations = remoteDestinations;
        return op;
    }

    public static ReplicatedOperation deleteMessage(long seqNum, String messageId, Set<String> destinations) {
        var op = new ReplicatedOperation();
        op.type = Type.DELETE_MESSAGE;
        op.seqNum = seqNum;
        op.messageId = messageId;
        op.destinations = destinations;
        return op;
    }

    public static ReplicatedOperation remotePost(long seqNum, Long sid, String sourceDomain,
            Message message, List<String> localRecipients) {
        var op = new ReplicatedOperation();
        op.type = Type.REMOTE_POST;
        op.seqNum = seqNum;
        op.sid = sid;
        op.sourceDomain = sourceDomain;
        op.message = message;
        op.localRecipients = localRecipients;
        return op;
    }

    public static ReplicatedOperation remoteDelete(long seqNum, Long sid, String sourceDomain,
            String messageId) {
        var op = new ReplicatedOperation();
        op.type = Type.REMOTE_DELETE;
        op.seqNum = seqNum;
        op.sid = sid;
        op.sourceDomain = sourceDomain;
        op.messageId = messageId;
        return op;
    }

    public static ReplicatedOperation deleteInbox(long seqNum, String userName) {
        var op = new ReplicatedOperation();
        op.type = Type.DELETE_INBOX;
        op.seqNum = seqNum;
        op.userName = userName;
        return op;
    }

    public static ReplicatedOperation removeInboxEntry(long seqNum, String mid, String recipient) {
        var op = new ReplicatedOperation();
        op.type = Type.REMOVE_INBOX_ENTRY;
        op.seqNum = seqNum;
        op.messageId = mid;
        op.userName = recipient;
        return op;
    }

    public Type getType() { return type; }
    public void setType(Type type) { this.type = type; }
    public long getSeqNum() { return seqNum; }
    public void setSeqNum(long seqNum) { this.seqNum = seqNum; }
    public Long getSid() { return sid; }
    public void setSid(Long sid) { this.sid = sid; }
    public String getSourceDomain() { return sourceDomain; }
    public void setSourceDomain(String sourceDomain) { this.sourceDomain = sourceDomain; }
    public Message getMessage() { return message; }
    public void setMessage(Message message) { this.message = message; }
    public List<String> getLocalRecipients() { return localRecipients; }
    public void setLocalRecipients(List<String> localRecipients) { this.localRecipients = localRecipients; }
    public Set<String> getRemoteDestinations() { return remoteDestinations; }
    public void setRemoteDestinations(Set<String> remoteDestinations) { this.remoteDestinations = remoteDestinations; }
    public String getMessageId() { return messageId; }
    public void setMessageId(String messageId) { this.messageId = messageId; }
    public Set<String> getDestinations() { return destinations; }
    public void setDestinations(Set<String> destinations) { this.destinations = destinations; }
    public String getUserName() { return userName; }
    public void setUserName(String userName) { this.userName = userName; }
    public String getPublisherId() { return publisherId; }
    public void setPublisherId(String publisherId) { this.publisherId = publisherId; }
}
