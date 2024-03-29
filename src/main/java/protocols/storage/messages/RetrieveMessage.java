package protocols.storage.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;
import java.util.UUID;

public class RetrieveMessage extends ProtoMessage{

    public static final short MSG_ID = 201;

    private final UUID mid;
    private final Host sender;

    private final short toDeliver;
    private final BigInteger cid;




    @Override
    public String toString() {
        return "RetrieveMessage{" +
                "mid=" + mid +
                '}';
    }

    public RetrieveMessage(UUID mid, Host sender, short toDeliver, BigInteger cid) {
        super(MSG_ID);
        this.mid = mid;
        this.sender = sender;
        this.toDeliver = toDeliver;
        this.cid = cid;
    }

    public Host getSender() {
        return sender;
    }

    public UUID getMid() {
        return mid;
    }

    public BigInteger getCid() { return cid; }

    //ATENTION BIGINT 8 BYTES
    public static ISerializer<RetrieveMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(RetrieveMessage retrieveMessage, ByteBuf out) throws IOException {
            out.writeLong(retrieveMessage.mid.getMostSignificantBits());
            out.writeLong(retrieveMessage.mid.getLeastSignificantBits());
            Host.serializer.serialize(retrieveMessage.sender, out);
            out.writeShort(retrieveMessage.toDeliver);
            out.writeBytes(retrieveMessage.cid.toByteArray());


            /*out.writeInt(floodMessage.content.length);
            if (floodMessage.content.length > 0) {
                out.writeBytes(floodMessage.content);
            }*/
        }


        @Override
        public RetrieveMessage deserialize(ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            Host sender = Host.serializer.deserialize(in);
            short toDeliver = in.readShort();
            byte[] big = in.readBytes(20).array();
            BigInteger cid = new BigInteger(big);
            return new RetrieveMessage(mid, sender,toDeliver, cid);
        }
    };
}
