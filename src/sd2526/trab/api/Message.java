package sd2526.trab.api;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import jakarta.persistence.Column;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;

/**
 * Represents a message in the system.
 */
@Entity
public class Message {

	@Id
	private String id;	
	
	private String sender;
		
	private long creationTime;

	private String subject;	
	
	@Column(length = 16384)
	private String contents;
	
	@ElementCollection(fetch = FetchType.EAGER)
	private Set<String> destination;
	
	public Message() {
		this(null, null, Collections.emptySet(), null, null);
	}
	
	public Message(String sender, String destination, String subject, String contents) {
		this(null, sender, Set.of(destination), subject, contents);
	}
	
	public Message(String sender, Set<String> destinations, String subject, String contents) {
		this(null, sender, destinations, subject, contents);
	}

	public Message(String id, String sender, String destination, String subject, String contents) {
		this(id, sender, Set.of(destination), subject, contents);
	}
	
	public Message(String id, String sender, Set<String> destinations, String subject, String contents) {
		this.id = id;
		this.sender = sender;
		this.subject = subject;
		this.contents = contents;
		this.creationTime = System.currentTimeMillis();
		this.destination = new HashSet<String>(destinations);
	}

	public Message(Message other) {
		this.id = other.id;
		this.sender = other.sender;
		this.subject = other.subject;
		this.contents = other.contents;
		this.creationTime = other.creationTime;
		this.destination = other.destination;
	}
	
	public String getSender() {
		return sender;
	}
	
	public void setSender(String sender) {
		this.sender = sender;
	}
		
	public Set<String> getDestination() {
		return destination;
	}
	
	public void setDestination(Set<String> destination) {
		this.destination = new HashSet<>(destination);
	}
	
	public void addDestination(String destination) {
		this.destination.add(destination);
	}

	public long getCreationTime() {
		return creationTime;
	}

	public void setCreationTime(long creationTime) {
		this.creationTime = creationTime;
	}

	public String getSubject() {
		return subject;
	}

	public void setSubject(String subject) {
		this.subject = subject;
	}

	public String getContents() {
		return contents;
	}

	public void setContents(String contents) {
		this.contents = contents;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	@Override
	public String toString() {
		return "MSG [id=" + id + ", sender=" + sender + ", destination=" + destination + "]";
	}
		
	public Message cloneWithUserNotFound(String recipient) {
		var unknownUserError = "FAILED TO SEND %s TO %s: UNKNOWN USER".formatted(id, recipient);
		return new Message( "%s.%s".formatted(id, recipient), sender, senderAddress(), unknownUserError, contents);
	}
	
	public Message cloneWithTimeout(String recipient) {
		var unknownUserError = "FAILED TO SEND %s TO %s: TIMEOUT".formatted(id, recipient);
		return new Message( "%s.%s".formatted(id, recipient), sender, senderAddress(), unknownUserError, contents);
	}
	
	public String originId() {
		return "%s-%s".formatted( senderAddress(), creationTime );
	}
	
	public String senderAddress() {
		int i = sender.indexOf('<');
		if( i < 0 )
			return sender;
		else
			return sender.substring(i + 1, sender.indexOf('>'));
	}
	
	public String senderName() {
		return senderAddress().split("@", 2)[0];
	}	
}
