package net.rageevent.governor.server;

import net.rageevent.governor.api.Envelope;
import net.rageevent.governor.api.EnvelopeConnection;
import net.rageevent.governor.api.Transaction;
import net.rageevent.governor.api.server.GameMetaData;
import net.rageevent.governor.api.server.ServerProfile;
import net.rageevent.governor.api.server.ServerRole;
import net.rageevent.governor.api.server.ServerState;
import net.rageevent.governor.api.shipments.network.server.RegisterGameServer;
import net.rageevent.myriad.api.server.ServerModeType;
import net.rageevent.myriad.api.server.ServerType;

import java.net.SocketAddress;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public final class ConnectedServer implements EnvelopeConnection {
    private final String name;
    private final EnvelopeConnection connection;
    private final ServerProfile profile;
    private volatile ServerState state = ServerState.OPEN;
    private volatile int maxPlayers;
    private GameMetaData gameMetaData;

    ConnectedServer(String name, EnvelopeConnection connection, ServerProfile profile) {
        this.name = name;
        this.connection = connection;
        this.profile = profile;
        this.maxPlayers = profile.maxPlayers();
    }


    @Override
    public String name() {
        return name;
    }

    @Override
    public SocketAddress remote() {
        return connection.remote();
    }

    @Override
    public boolean active() {
        return connection.active();
    }

    public ServerType serverType() {
        return profile.serverType();
    }
    public ServerModeType modeType(){
        return profile.modeType();
    }
    public ServerProfile profile() {
        return profile;
    }

    public ServerState state() {
        return state;
    }

    public boolean isProxy() {
        return profile.role() == ServerRole.PROXY;
    }

    public boolean isGameServer() {
        return profile.role() == ServerRole.GAME_SERVER;
    }

    public boolean is(EnvelopeConnection other) {
        return other == this || other == connection;
    }

    public int maxPlayers() {
        return maxPlayers;
    }

    public GameMetaData gameData(){
        return gameMetaData;
    }
    public void gameData(GameMetaData metaData){
        this.gameMetaData = metaData;
    }

    public void update(ServerState newState, int newPlayerLimit) {
        this.state = newState;
        this.maxPlayers = newPlayerLimit;
    }

    @Override
    public void send(Envelope envelope) {
        connection.send(envelope);
    }

    @Override
    public <E extends Envelope> CompletableFuture<E> request(Transaction<E> transaction, Duration duration) {
        return connection.request(transaction, duration);
    }

    @Override
    public void close() {
        connection.close();
    }

    @Override
    public String toString() {
        return "$name (${profile.role().name().toLowerCase()}, ${serverType()})";
    }


}
