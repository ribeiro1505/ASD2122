package protocols.apps.kademlia;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.Arrays;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

public class KademliaPingMessage extends ProtoMessage {

	public static final short MSG_ID = 701;

	private final KademliaNode sender;
	private final BigInteger messageId;
	private final BigInteger echoedId;

	public KademliaPingMessage(KademliaNode sender, BigInteger messageId, BigInteger echoedId) {
		super(MSG_ID);
		this.sender = sender;
		this.messageId = messageId;
		this.echoedId = echoedId;
	}
	public KademliaPingMessage(KademliaNode sender, BigInteger messageId) {
		super(MSG_ID);
		this.sender = sender;
		this.messageId = messageId;
		byte[] blankId = new byte[20];
		Arrays.fill(blankId, (byte)0xf);
		this.echoedId = new BigInteger(blankId);
	}
	
	public KademliaNode getSender() {
		return this.sender;
	}

	public BigInteger getMessageId() {
		return this.messageId;
	}
	
	public BigInteger getEchoedId() {
		return this.echoedId;
	}

	// |MSG_TYPE|SENDER_ID|MESSAGE_ID|ECHOED_ID|HOST|
	public static ISerializer<KademliaPingMessage> serializer = new ISerializer<>() {
		@Override
		public void serialize(KademliaPingMessage msg, ByteBuf out) throws IOException {
			out.writeShort(MSG_ID);
			out.writeBytes(msg.getSender().getKey().toByteArray());
			out.writeBytes(msg.getMessageId().toByteArray());
			out.writeBytes(msg.getEchoedId().toByteArray());
			Host.serializer.serialize(msg.getSender().getHost(), out);
		}

		@Override
		public KademliaPingMessage deserialize(ByteBuf in) throws IOException {
			short messageType = in.readShort();
			byte[] buf = new byte[20];
			in.readBytes(buf);
			BigInteger senderId = new BigInteger(buf);
			in.readBytes(buf);
			BigInteger messageId = new BigInteger(buf);
			in.readBytes(buf);
			BigInteger echoedId = new BigInteger(buf);
			Host host = Host.serializer.deserialize(in);
			KademliaNode node = new KademliaNode(senderId, host);
			return new KademliaPingMessage(node, messageId, echoedId);
		}
	};

	public static void main(String[] args) throws Exception{
		Host host = new Host(InetAddress.getByName("127.0.0.1"), 1234);
		KademliaNode node = new KademliaNode(host);
		KademliaPingMessage ping = new KademliaPingMessage(node, node.getKey());
		System.out.println(ping.getSender());
		System.out.println(ping.getMessageId());
		System.out.println(ping.getEchoedId());
		
		ByteBuf buf = Unpooled.buffer();
		KademliaPingMessage.serializer.serialize(ping, buf);
		
		KademliaPingMessage ping2 = KademliaPingMessage.serializer.deserialize(buf);
		System.out.println(ping2.getSender());
		System.out.println(ping2.getMessageId());
		System.out.println(ping2.getEchoedId());
	}
}
