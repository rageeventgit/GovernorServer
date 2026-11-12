package net.rageevent.governor.server.modules;

import lombok.RequiredArgsConstructor;
import net.rageevent.governor.api.Envelope;
import net.rageevent.governor.api.EnvelopeAgent;
import net.rageevent.governor.api.EnvelopeCarrier;
import net.rageevent.governor.api.EnvelopeConnection;
import net.rageevent.governor.api.protocol.ConnectionListener;
import net.rageevent.governor.api.shipments.network.player.*;
import net.rageevent.governor.api.shipments.network.player.data.PullUserData;
import net.rageevent.governor.api.shipments.network.server.*;
import net.rageevent.governor.server.ConnectedServer;
import net.rageevent.governor.server.ServerConnectionManager;
import net.rageevent.governor.server.players.OnlinePlayer;
import net.rageevent.governor.server.players.PlayerTracker;

import java.util.Optional;
import java.util.UUID;

@RequiredArgsConstructor
public class NetworkModule implements EnvelopeCarrier, ConnectionListener {

    private static final System.Logger LOG = System.getLogger(NetworkModule.class.getName());

    private final ServerConnectionManager servers;
    private final PlayerTracker players;

    @Override
    public void register(EnvelopeAgent agent) {
        agent.on(UpdateServerState.class, (connection, update) ->
                        servers.of(connection).ifPresent(server -> server.update(update.state(), update.maxPlayers())))
                .on(PlayerJoinedNetwork.class, (connection, event) ->
                        fromProxy(connection, event).ifPresent(proxy -> players.join(proxy, event.player(), event.name(), event.server())))
                .on(PlayerSwitchedServer.class, (connection, event) ->
                        fromProxy(connection, event).ifPresent(proxy -> players.switchServer(proxy, event.player(), event.server())))
                .on(PlayerLeftNetwork.class, (connection, event) ->
                        fromProxy(connection, event).ifPresent(proxy -> players.leave(proxy, event.player())))
                .on(ProxyRoster.class, (connection, roster) -> fromProxy(connection, roster).ifPresent(proxy -> players.replaceRoster(proxy, roster.players())))
                .on(SupplyGameProfile.class, (_, gameProfile) -> servers.get(gameProfile.name()).ifPresent(server -> server.gameData(gameProfile.metaData())))
                .onAsyncTransaction(SendToServerRequest.class, (_, request) -> players.transfer(request.player(), request.server()))
                .onAsyncTransaction(SendToGroupRequest.class, (_, request) -> players.sendToGroup(request.player(), request.group()))
                .onTransaction(FindPlayerRequest.class, (_, request) -> find(request.name()))
        ;
    }

    private FindPlayerResult find(String name) {
        Optional<OnlinePlayer> online = players.find(name);
        if (online.isPresent()) {
            OnlinePlayer player = online.get();
            return new FindPlayerResult(player.uuid(), player.name(), true, player.proxy(), player.server());
        }
        Optional<UUID> known = players.resolve(name);
        return known.map(uuid -> new FindPlayerResult(uuid, players.nameOf(uuid).orElse(name), false, null, null))
                .orElseGet(() -> new FindPlayerResult(null, null, false, null, null));
    }

    private Optional<String> fromProxy(EnvelopeConnection connection, Envelope event) {
        Optional<ConnectedServer> server = servers.of(connection).filter(ConnectedServer::isProxy);
        if (server.isEmpty()) {
            LOG.log(System.Logger.Level.WARNING, "Ignoring ${event.getClass().getSimpleName()} from $connection: only proxies report players");
        }
        return server.map(ConnectedServer::name);
    }

    private void loadPlayerData(PullUserData request) {
        players.find(request.player()).flatMap(player -> servers.get(player.server())).ifPresent(connectedServer -> {
            connectedServer.send(request);
        });


    }

    @Override
    public void ready(EnvelopeConnection connection) {
        servers.of(connection).ifPresent(server -> {
            if (server.isGameServer())
                servers.broadcastToProxies(added(server));
            else
                servers.gameServers().forEach(backend -> server.send(added(backend)));
        });
    }

    @Override
    public void disconnect(EnvelopeConnection connection) {
        servers.of(connection).ifPresent(server -> {
            if (server.isGameServer())
                servers.broadcastToProxies(new RemoveGameServer(server.name()));
            else
                players.proxyLost(server.name());
        });
    }

    private static RegisterGameServer added(ConnectedServer server) {
        return new RegisterGameServer(server.name(), server.serverType(), server.profile().host(), server.profile().port());
    }
}
