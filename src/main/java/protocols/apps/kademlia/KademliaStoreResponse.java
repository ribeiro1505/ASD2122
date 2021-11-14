package protocols.apps.kademlia;

import java.io.IOException;
import java.math.BigInteger;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

public class KademliaStoreResponse extends ProtoMessage{

	public static final short MSG_ID = 706;
	private final KademliaNode sender;
	private final BigInteger targetId;
	private final BigInteger messageId;
	private final BigInteger echoedId;
	
	public KademliaStoreResponse(KademliaNode sender, BigInteger target, BigInteger messageId, BigInteger echoedId) {
		super(MSG_ID);
		this.sender = sender;
		this.targetId = target;
		this.messageId = messageId;
		this.echoedId = echoedId;
	}

	public KademliaNode getSender() {
		return sender;
	}

	public BigInteger getTargetId() {
		return targetId;
	}

	public BigInteger getMessageId() {
		return messageId;
	}

	public BigInteger getEchoedId() {
		return echoedId;
	}
	
	// |MSG_TYPE|SENDER_ID|HOST|TARGET_ID|MESSAGE_ID|ECHOED_ID|
	public static ISerializer<KademliaStoreResponse> serializer = new ISerializer<>() {
		@Override
		public void serialize(KademliaStoreResponse msg, ByteBuf out) throws IOException {
			out.writeShort(MSG_ID);
			out.writeBytes(msg.getSender().getKey().toByteArray());
			Host.serializer.serialize(msg.getSender().getHost(), out);
			out.writeBytes(msg.getTargetId().toByteArray());
			out.writeBytes(msg.getMessageId().toByteArray());
			out.writeBytes(msg.getEchoedId().toByteArray());
		}

		@Override
		public KademliaStoreResponse deserialize(ByteBuf in) throws IOException {
			short msgType = in.readShort();
			byte[] buffer = new byte[20];
			in.readBytes(buffer);
			BigInteger senderId = new BigInteger(buffer);
			Host host = Host.serializer.deserialize(in);
			KademliaNode sender = new KademliaNode(senderId, host);
			in.readBytes(buffer);
			BigInteger targetId = new BigInteger(buffer);
			in.readBytes(buffer);
			BigInteger messageId = new BigInteger(buffer);
			in.readBytes(buffer);
			BigInteger echoedId = new BigInteger(buffer);
			return new KademliaStoreResponse(sender, targetId, messageId, echoedId);
		}
	};
}
