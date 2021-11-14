package protocols.apps.kademlia;

import java.io.IOException;
import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import channel.notifications.ChannelCreated;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.ChannelMetrics;
import pt.unl.fct.di.novasys.channel.tcp.events.InConnectionDown;
import pt.unl.fct.di.novasys.channel.tcp.events.InConnectionUp;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionDown;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionFailed;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionUp;
import pt.unl.fct.di.novasys.network.data.Host;

public class KademliaProtocol extends GenericProtocol {

	private static final Logger logger = LogManager.getLogger(KademliaProtocol.class);

	public static final String PROTO_NAME = "KademliaApplication";
	public static final short PROTO_ID = 701;

	// Kademlia parameters
	// TODO Move to config
	private int K;
	private int ALPHA;
	private String KEY_ALGO;
	private int KEY_SIZE;

	private int channelId;
	private final Host self;
	private final KademliaNode selfNode;
	private Map<Integer, Host> connectionsIn;
	private Map<Integer, Host> connectionsOut;
	private Map<BigInteger, KademliaLookupCache> lookupCache;
	private boolean isBootstrap = false;

	private KademliaBucket[] kBuckets;
	private Random messageIdGen;

	public KademliaProtocol(String protoName, short protoId, Host self)
			throws NoSuchAlgorithmException, HandlerRegistrationException {
		super(protoName, protoId);
		this.self = self;
		this.selfNode = new KademliaNode(this.self);
		this.messageIdGen = new Random();
		this.connectionsIn = new HashMap<>();
		this.connectionsOut = new HashMap<>();
		this.lookupCache = new HashMap<>();
	}

	@Override
	public void init(Properties properties) throws HandlerRegistrationException, IOException {
		triggerNotification(new ChannelCreated(channelId));

		this.K = Integer.parseInt(properties.getProperty("kad_k", "20"));
		this.ALPHA = Integer.parseInt(properties.getProperty("kad_alpha", "3"));
		this.KEY_ALGO = properties.getProperty("kad_hashalgo", "SHA-1");
		this.KEY_SIZE = Integer.parseInt(properties.getProperty("kad_keysize", "160"));

		String cMetricsInterval = properties.getProperty("channel_metrics_interval", "10000"); // 10 seconds

		// Create a properties object to setup channel-specific properties. See the
		// channel description for more details.
		Properties channelProps = new Properties();
		channelProps.setProperty(TCPChannel.ADDRESS_KEY, properties.getProperty("address")); // The address to bind to
		channelProps.setProperty(TCPChannel.PORT_KEY, properties.getProperty("port")); // The port to bind to
		channelProps.setProperty(TCPChannel.METRICS_INTERVAL_KEY, cMetricsInterval); // The interval to receive channel
																						// metrics
		channelProps.setProperty(TCPChannel.HEARTBEAT_INTERVAL_KEY, "1000"); // Heartbeats interval for established
																				// connections
		channelProps.setProperty(TCPChannel.HEARTBEAT_TOLERANCE_KEY, "3000"); // Time passed without heartbeats until
																				// closing a connection
		channelProps.setProperty(TCPChannel.CONNECT_TIMEOUT_KEY, "1000"); // TCP connect timeout
		channelId = createChannel(TCPChannel.NAME, channelProps); // Create the channel with the given properties

		String addressAndPort = properties.getProperty("address")+":"+properties.getProperty("port");
		this.isBootstrap = addressAndPort.equals(properties.getProperty("kad_bootstrap"));
		
		kBuckets = new KademliaBucket[KEY_SIZE];
		for (int i = 0; i < KEY_SIZE; i++) {
			kBuckets[i] = new KademliaBucket(K, i);
		}

		/*--------------------- Register Request Handlers ---------------------*/

		/*-------------------- Register Message Serializers -------------------*/
		registerMessageSerializer(channelId, KademliaPingMessage.MSG_ID, KademliaPingMessage.serializer);
		registerMessageSerializer(channelId, KademliaPingResponse.MSG_ID, KademliaPingResponse.serializer);
		registerMessageSerializer(channelId, KademliaFindNodeMessage.MSG_ID, KademliaFindNodeMessage.serializer);
		registerMessageSerializer(channelId, KademliaFindNodeResponse.MSG_ID, KademliaFindNodeResponse.serializer);
		registerMessageSerializer(channelId, KademliaStoreMessage.MSG_ID, KademliaStoreMessage.serializer);
		registerMessageSerializer(channelId, KademliaStoreResponse.MSG_ID, KademliaStoreResponse.serializer);

		/*--------------------- Register Message Handlers ---------------------*/
		registerMessageHandler(channelId, KademliaPingMessage.MSG_ID, this::uponPingMessage);
		registerMessageHandler(channelId, KademliaPingResponse.MSG_ID, this::uponPingResponse);
		registerMessageHandler(channelId, KademliaFindNodeMessage.MSG_ID, this::uponFindNodeMessage);
		registerMessageHandler(channelId, KademliaFindNodeResponse.MSG_ID, this::uponFindNodeResponse);
		registerMessageHandler(channelId, KademliaStoreMessage.MSG_ID, this::uponStoreMessage);
		registerMessageHandler(channelId, KademliaStoreResponse.MSG_ID, this::uponStoreResponse);

		/*---------------------- Register Timer Handlers ----------------------*/
		registerTimerHandler(KademliaLookupTimer.TIMER_ID, this::uponLookupTimer);

		/*---------------------- Register Channel Events ----------------------*/
		registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
		registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
		registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
		registerChannelEventHandler(channelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
		registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);
		registerChannelEventHandler(channelId, ChannelMetrics.EVENT_ID, this::uponChannelMetrics);

	}

	private static BigInteger calculateXORDistance(BigInteger a, BigInteger b) {
		return a.xor(b).abs();
	}

	private static int getBucket(BigInteger key1, BigInteger key2) {
		double d = Math.log(calculateXORDistance(key1, key2).doubleValue()) / Math.log(2);
		// Should be in the range of the number of bits in the key, normally 160
		// so it should be safe to cast to int.
		return (int) Math.floor(d);
	}

	private BigInteger makeRandomId() {
		byte[] out = new byte[20];
		messageIdGen.nextBytes(out);
		return new BigInteger(out);
	}

	private void updateContact(KademliaBucket kb, KademliaNode node) {
		if (kb.getFreeSpots() > 0 || kb.getContact(node.getKey()) != null) {
			kb.insertContact(node);
		}
		if (kb.getFreeSpots() < 1) {
			KademliaPingMessage ping = new KademliaPingMessage(selfNode, makeRandomId());
			trySendMessage(ping, node.getHost());
			kb.insertContact(node);
		}
	}

	private void lookupNode(BigInteger id) {
		if (id.compareTo(selfNode.getKey()) == 0) {
			return;
		}

		int targetBucket = getBucket(selfNode.getKey(), id);
		List<KademliaNode> nodes = new ArrayList<KademliaNode>();
		nodes.addAll(kBuckets[targetBucket].getNContacts(ALPHA));
		int idx = 0;
		while (nodes.size() < ALPHA && idx < kBuckets.length) { // If not enough on the buckets get the closest
			nodes.addAll(kBuckets[idx].getNContacts(ALPHA - nodes.size()));
			idx++;
		}

		if (!lookupCache.containsKey(id)) {
			lookupCache.put(id, new KademliaLookupCache(selfNode, id, nodes));
		}
	}

	private void uponFindNodeMessage(KademliaFindNodeMessage msg, Host from, short sourceProto, int channelId) {
		BigInteger senderId = msg.getSender().getKey();
		int kBucket = getBucket(selfNode.getKey(), senderId);
		updateContact(kBuckets[kBucket], new KademliaNode(senderId, from));

		KademliaNode targetNode = kBuckets[getBucket(selfNode.getKey(), msg.getTarget())].getContact(msg.getTarget());
		if (targetNode != null) {
			List<KademliaNode> nodesToSend = new ArrayList<>();
			nodesToSend.add(targetNode);
			trySendMessage(new KademliaFindNodeResponse(selfNode, msg.getTarget(), makeRandomId(), msg.getMessageId(),
					nodesToSend), from);
		} else {
			lookupNode(msg.getTarget());
		}
	}

	private void uponFindNodeResponse(KademliaFindNodeResponse msg, Host from, short sourceProto, int channelId) {
		BigInteger senderId = msg.getSender().getKey();
		int kBucket = getBucket(selfNode.getKey(), senderId);
		updateContact(kBuckets[kBucket], new KademliaNode(senderId, from));

		KademliaLookupCache cache = lookupCache.get(msg.getTargetId());
		if (cache != null) {
			for (KademliaNode n : msg.getNodes()) {
				int targetBucket = getBucket(selfNode.getKey(), n.getKey());
				updateContact(kBuckets[targetBucket], n);
				cache.addNode(n);
			}
		}
	}

	// Periodically check the lookup cache and chooses ALPHA entries to send find
	// requests
	private void uponLookupTimer(KademliaLookupTimer timer, long timerId) {
		for (BigInteger key : lookupCache.keySet()) {
			if (lookupCache.get(key).hasFound() == null) {
				for (int i = 0; i < ALPHA; i++) {
					KademliaFindNodeMessage msg = new KademliaFindNodeMessage(selfNode, key, makeRandomId());
					trySendMessage(msg, self);
				}
			} else {
				List<KademliaNode> node = List.of(lookupCache.get(key).hasFound());
				KademliaFindNodeResponse res = new KademliaFindNodeResponse(selfNode, key, makeRandomId(),
						makeRandomId(), node);
				trySendMessage(res, lookupCache.get(key).getRequester().getHost());
			}
		}
	}

	private void uponPingMessage(KademliaPingMessage msg, Host from, short sourceProto, int channelId) {
		BigInteger senderId = msg.getSender().getKey();
		int kBucket = getBucket(selfNode.getKey(), senderId);
		updateContact(kBuckets[kBucket], new KademliaNode(senderId, from));
		KademliaPingResponse response = new KademliaPingResponse(selfNode, makeRandomId(), senderId);
		trySendMessage(response, msg.getSender().getHost());
	}

	private void uponPingResponse(KademliaPingResponse msg, Host from, short sourceProto, int channelId) {
		BigInteger senderId = msg.getSender().getKey();
		int kBucket = getBucket(selfNode.getKey(), senderId);
		updateContact(kBuckets[kBucket], new KademliaNode(senderId, from));
	}

	private void uponStoreMessage(KademliaStoreMessage msg, Host from, short sourceProto, int channelId) {
		BigInteger senderId = msg.getSender().getKey();
		int kBucket = getBucket(selfNode.getKey(), senderId);
		updateContact(kBuckets[kBucket], new KademliaNode(senderId, from));
		
		
	}
	
	private void uponStoreResponse(KademliaStoreResponse msg, Host from, short sourceProto, int channelId) {
		BigInteger senderId = msg.getSender().getKey();
		int kBucket = getBucket(selfNode.getKey(), senderId);
		updateContact(kBuckets[kBucket], new KademliaNode(senderId, from));
		
		
	}

	private void trySendMessage(ProtoMessage message, Host destination) {
		if (!connectionsOut.containsKey(destination.hashCode())) {
			openConnection(destination);
		}
		sendMessage(message, destination);
	}

	
	// TCP events handlers, mainly for logging as Kademlia is meant to work
	// over UDP and dosen't make use of TCP amenities
	private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
		Host peer = event.getNode();
		logger.debug("Connection to {} is down cause {}", peer, event.getCause());
		connectionsOut.remove(peer.hashCode());
	}

	private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
		Host peer = event.getNode();
		logger.debug("Connection to {} failed cause: {}", peer, event.getCause());
		connectionsOut.remove(peer.hashCode());
	}

	private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
		Host peer = event.getNode();
		logger.debug("Connection to {} is up", peer);
		connectionsOut.put(peer.hashCode(), peer);
	}

	private void uponInConnectionUp(InConnectionUp event, int channelId) {
		Host peer = event.getNode();
		logger.debug("Connection from {} is up", peer);
		connectionsIn.put(peer.hashCode(), peer);
	}

	private void uponInConnectionDown(InConnectionDown event, int channelId) {
		Host peer = event.getNode();
		logger.debug("Connection from {} is down cause {}", peer, event.getCause());
		connectionsIn.remove(peer.hashCode());
	}

	private void uponChannelMetrics(ChannelMetrics event, int channelId) {
		StringBuilder sb = new StringBuilder("Channel Metrics:\n");
		sb.append("In channels:\n");
		event.getInConnections()
				.forEach(c -> sb.append(
						String.format("\t%s: msgOut=%s (%s) msgIn=%s (%s)\n", c.getPeer(), c.getSentAppMessages(),
								c.getSentAppBytes(), c.getReceivedAppMessages(), c.getReceivedAppBytes())));
		event.getOldInConnections()
				.forEach(c -> sb.append(
						String.format("\t%s: msgOut=%s (%s) msgIn=%s (%s) (old)\n", c.getPeer(), c.getSentAppMessages(),
								c.getSentAppBytes(), c.getReceivedAppMessages(), c.getReceivedAppBytes())));
		sb.append("Out channels:\n");
		event.getOutConnections()
				.forEach(c -> sb.append(
						String.format("\t%s: msgOut=%s (%s) msgIn=%s (%s)\n", c.getPeer(), c.getSentAppMessages(),
								c.getSentAppBytes(), c.getReceivedAppMessages(), c.getReceivedAppBytes())));
		event.getOldOutConnections()
				.forEach(c -> sb.append(
						String.format("\t%s: msgOut=%s (%s) msgIn=%s (%s) (old)\n", c.getPeer(), c.getSentAppMessages(),
								c.getSentAppBytes(), c.getReceivedAppMessages(), c.getReceivedAppBytes())));
		sb.setLength(sb.length() - 1);
		logger.info(sb);
	}

	public static void main(String[] args) throws Exception {

	}

}
