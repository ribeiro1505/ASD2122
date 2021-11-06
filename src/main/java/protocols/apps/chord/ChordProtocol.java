package protocols.apps.chord;

import channel.notifications.ChannelCreated;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.dht.messages.FindSuccessorMessage;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import pt.unl.fct.di.novasys.network.data.Host;
import utils.HashProducer;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

public class ChordProtocol extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(ChordProtocol.class);

    public static final String PROTO_NAME = "ChordApplication";
    public static final short PROTO_ID = 301;


    private Host predecessor, successor;
    private int selfID;
    //private HashMap<Long, ChordProtocol> fingerTable;
    private boolean hasFailed;
    private long next;
    private Host self;
    private final HashMap<Long,Host> fingerTable; //Peers I am connected to

    private final int sampleTime; //param: timeout for samples
    private final int subsetSize; //param: maximum size of sample;

    //Variables related with measurement
    private long storeRequests = 0;
    private long storeRequestsCompleted = 0;
    private long retrieveRequests = 0;
    private long retrieveRequestsSuccessful = 0;
    private long retrieveRequestsFailed = 0;

    private final int channelId; //Id of the created channel

    public ChordProtocol(Properties properties,Host self) throws IOException {
        super(PROTO_NAME, PROTO_ID);
        this.self=self;
        this.selfID= self.hashCode();
        this.fingerTable = new HashMap<Long,Host>();
        predecessor = null;
        successor = null;


        //Get some configurations from the Properties object
        this.subsetSize = Integer.parseInt(properties.getProperty("sample_size", "6"));
        this.sampleTime = Integer.parseInt(properties.getProperty("sample_time", "2000")); //2 seconds

        String cMetricsInterval = properties.getProperty("channel_metrics_interval", "10000"); //10 seconds

        //Create a properties object to setup channel-specific properties. See the channel description for more details.
        Properties channelProps = new Properties();
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, properties.getProperty("address")); //The address to bind to
        channelProps.setProperty(TCPChannel.PORT_KEY, properties.getProperty("port")); //The port to bind to
        channelProps.setProperty(TCPChannel.METRICS_INTERVAL_KEY, cMetricsInterval); //The interval to receive channel metrics
        channelProps.setProperty(TCPChannel.HEARTBEAT_INTERVAL_KEY, "1000"); //Heartbeats interval for established connections
        channelProps.setProperty(TCPChannel.HEARTBEAT_TOLERANCE_KEY, "3000"); //Time passed without heartbeats until closing a connection
        channelProps.setProperty(TCPChannel.CONNECT_TIMEOUT_KEY, "1000"); //TCP connect timeout
        channelId = createChannel(TCPChannel.NAME, channelProps); //Create the channel with the given properties

        /*---------------------- Register Message Serializers ---------------------- */
        registerMessageSerializer(channelId, FindSuccessorMessage.MSG_ID, FindSuccessorMessage.serializer);

        /*---------------------- Register Message Handlers -------------------------- */
        registerMessageHandler(channelId, SampleMessage.MSG_ID, this::uponSample, this::uponMsgFail);

        /*--------------------- Register Timer Handlers ----------------------------- */
        registerTimerHandler(SampleTimer.TIMER_ID, this::uponSampleTimer);//Stabilize / finger refresh
        registerTimerHandler(InfoTimer.TIMER_ID, this::uponInfoTime);

        /*-------------------- Register Channel Events ------------------------------- */
        registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);//avisar o sucessor do sucessor que falhou
        registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);//Fazer join ou notify?
        //TODO ENQUANTO O SUCESSOR FOR MENOR QUE EU FAZER JOIN DA RESPOSTA
        //registerChannelEventHandler(channelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        //registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);
        //registerChannelEventHandler(channelId, ChannelMetrics.EVENT_ID, this::uponChannelMetrics);
    }

    @Override
    public void init(Properties properties) throws HandlerRegistrationException, IOException {
        //Inform the dissemination protocol about the channel we created in the constructor
        triggerNotification(new ChannelCreated(channelId));

        //If there is a contact node, attempt to establish connection
        if (properties.containsKey("contact")) {
            try {
                String contact = properties.getProperty("contact");
                String[] hostElems = contact.split(":");
                Host contactHost = new Host(InetAddress.getByName(hostElems[0]), Short.parseShort(hostElems[1]));
                //We add to the pending set until the connection is successful
                //pending.add(contactHost);
                openConnection(contactHost);
            } catch (Exception e) {
                logger.error("Invalid contact on configuration: '" + properties.getProperty("contacts"));
                e.printStackTrace();
                System.exit(-1);
            }
        }

        //Setup the timer used to send samples (we registered its handler on the constructor)
        setupPeriodicTimer(new SampleTimer(), this.sampleTime, this.sampleTime);

        //Setup the timer to display protocol information (also registered handler previously)
        int pMetricsInterval = Integer.parseInt(props.getProperty("protocol_metrics_interval", "10000"));
        if (pMetricsInterval > 0)
            setupPeriodicTimer(new InfoTimer(), pMetricsInterval, pMetricsInterval);
    }

    public void chordCreate() {

    }

    public void chordJoin(ChordProtocol n) {
        predecessor = null;
        successor = n.findSuccessor(this.selfID);
    }

    public void chordStabilized() {
        ChordProtocol x = successor.predecessor;
        if (x.selfID > this.selfID && x.selfID < this.successor.selfID)
            this.successor = x;

        successor.chordNotify(this);
    }

    public void chordNotify(ChordProtocol n) {
        if (this.predecessor == null || (n.selfID > predecessor.selfID && n.selfID < n.selfID))
            this.predecessor = n;
    }

    public void chordFixedFingers() {
        next = next + 1;
        if (next > fingerTable.size())
            next = 1;
        fingerTable.replace(next, findSuccessor(this.selfID + 2 ^ (next - 1)));
    }

    public void chordCheckPredecessor() {
        if (predecessor.hasFailed)
            predecessor = null;
    }

    public ChordProtocol findSuccessor(long id) {
        if (id > this.selfID && id <= successor.selfID)
            return successor;

        ChordProtocol n = closestPrecedingNode(id);
        return n.findSuccessor(id);
    }

    public ChordProtocol closestPrecedingNode(long id) {
        for (long i = fingerTable.size() - 1; i > 1; i--)
            if (id > this.selfID && fingerTable.get(i).selfID <= id)
                return fingerTable.get(i);

        return this;
    }

    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        Host peer = event.getNode();
        logger.debug("Connection to {} is up", peer);
        //Verificar se ID maior que o meu, se for
        if(self.compareTo(peer)>0){
            UUID uuid = UUID.randomUUID();
            FindSuccessorMessage findSuccessorMessage = new FindSuccessorMessage(uuid,self,PROTO_ID);
            sendMessage(findSuccessorMessage,peer);
        }else{
            successor=peer;
        }

    }

}
