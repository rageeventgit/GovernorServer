package net.rageevent.governor.server;

import net.rageevent.governor.api.Envelope;
import net.rageevent.governor.api.EnvelopeConnection;
import net.rageevent.governor.api.Transaction;
import net.rageevent.governor.api.server.ServerProfile;
import net.rageevent.myriad.server.ServerModeType;
import net.rageevent.myriad.server.ServerType;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;

public class ServerConnectionManager {

    private final ConcurrentMap<String, ConnectedServer> servers = new ConcurrentHashMap<>();

    public Optional<ConnectedServer> get(String name) {
        return name == null ? Optional.empty() : Optional.ofNullable(servers.get(name));
    }

    public Optional<ConnectedServer> of(EnvelopeConnection connection) {
        return get(connection.name()).filter(server -> server.is(connection)).stream().findFirst();
    }

    public List<ConnectedServer> all() {
        return List.copyOf(servers.values());
    }

    public List<ConnectedServer> proxies() {
        return servers.values().stream().filter(ConnectedServer::isProxy).toList();
    }

    public List<ConnectedServer> gameServers() {
        return servers.values().stream().filter(ConnectedServer::isGameServer).toList();
    }

    public List<ConnectedServer> inGroup(ServerType group) {
        return servers.values().stream().filter(server -> server.isGameServer() && server.serverType() == group).toList();
    }

    public List<ConnectedServer> inMode(ServerModeType group) {
        return servers.values().stream().filter(server -> server.isGameServer() && server.serverType() == group.type() && server.modeType() == group).toList();
    }

    public Set<String> names() {
        return Set.copyOf(servers.keySet());
    }

    public int size() {
        return servers.size();
    }

    public void broadcast(Envelope Envelope) {
        broadcast(Envelope, server -> true);
    }

    public void broadcastExcept(EnvelopeConnection excluded, Envelope Envelope) {
        broadcast(Envelope, server -> !server.is(excluded));
    }

    public void broadcastToProxies(Envelope Envelope) {
        broadcast(Envelope, ConnectedServer::isProxy);
    }

    public void broadcast(Envelope Envelope, Predicate<? super ConnectedServer> filter) {
        for (ConnectedServer server : servers.values()) {
            if (filter.test(server)) server.send(Envelope);
        }
    }

    public <R extends Envelope> Map<String, CompletableFuture<R>> requestAll(Transaction<R> request) {
        Map<String, CompletableFuture<R>> responses = new LinkedHashMap<>();
        servers.forEach((name, server) -> responses.put(name, server.request(request)));
        return responses;
    }

    String claim(String name, ServerProfile profile, EnvelopeConnection connection) {
        boolean[] taken = {false};
        servers.compute(name, (_, existing) -> {
            if (existing != null && existing.active()) {
                taken[0] = true;
                return existing;
            }
            return new ConnectedServer(name, connection, profile);
        });
        return taken[0] ? "A server named '$name' is already connected" : null;
    }

    void release(EnvelopeConnection connection) {
        if (connection.name() == null) return;
        servers.computeIfPresent(connection.name(), (_, server) -> server.is(connection) ? null : server);
    }
}
