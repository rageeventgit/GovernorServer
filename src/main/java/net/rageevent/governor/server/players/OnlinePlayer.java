package net.rageevent.governor.server.players;

import java.util.UUID;

/**
 * @param proxy  the proxy the player is connected through
 * @param server the backend they are on; {@code null} while still logging in
 */
public record OnlinePlayer(UUID uuid, String name, String proxy, String server) {

    OnlinePlayer withServer(String server) {
        return new OnlinePlayer(uuid, name, proxy, server);
    }
}
