package twizzy.tech.clerk.commands

import com.velocitypowered.api.proxy.Player
import net.kyori.adventure.text.Component
import net.kyori.adventure.text.format.NamedTextColor
import revxrsal.commands.annotation.Command
import twizzy.tech.clerk.Clerk
import twizzy.tech.clerk.util.JaSync
import twizzy.tech.clerk.util.CacheFallback
import kotlinx.coroutines.runBlocking
import revxrsal.commands.annotation.Cooldown
import revxrsal.commands.velocity.annotation.CommandPermission
import twizzy.tech.clerk.player.Account
import java.util.concurrent.TimeUnit

class LastSeen(private val clerk: Clerk)  {
    private val cacheFallback = CacheFallback(clerk.lettuce, clerk.jaSync, clerk.logger)

    @Command("lastseen <username>")
    @CommandPermission("clerk.lastseen")
    @Cooldown(value = 5, unit = TimeUnit.SECONDS)
    fun lastSeen(actor: Player, @Account.CachedAccounts username: String) {
        runBlocking {
            // First check if the player is currently online
            val targetPlayer = clerk.server.getPlayer(username).orElse(null)
            if (targetPlayer != null && targetPlayer.isActive) {
                actor.sendMessage(
                    Component.text("$username is currently online", NamedTextColor.GREEN)
                )
                return@runBlocking
            }

            // Try to get last_seen data from cache (Redis first, then PostgreSQL)
            val lastSeenTimestamp = cacheFallback.getData<Long>(
                username = username,
                column = "last_seen"
            ) { rawValue ->
                when (rawValue) {
                    is Number -> rawValue.toLong()
                    is String -> rawValue.toLongOrNull() ?: 0L
                    else -> 0L
                }
            }

            if (lastSeenTimestamp != null && lastSeenTimestamp > 0L) {
                // Format the duration nicely
                val formattedTime = cacheFallback.formatDuration(lastSeenTimestamp, useAgo = true)
                actor.sendMessage(
                    Component.text("$username was last seen $formattedTime", NamedTextColor.YELLOW)
                )
            } else {
                actor.sendMessage(
                    Component.text("No last seen data found for $username", NamedTextColor.RED)
                )
            }
        }
    }

    @Command("test <player>")
    fun testPlayer(actor: Player, @Account.CachedAccounts player : String) {
        // Get player object if online
        val playerObj = clerk.server.getPlayer(player).orElse(null)
        clerk.server.allPlayers

        if (playerObj != null) {
            actor.sendMessage(Component.text("Online player: ${playerObj.username}", NamedTextColor.GREEN))
        } else {
            // Handle case where player name is provided but player isn't online
            actor.sendMessage(Component.text("Player '$player' is not online or doesn't exist", NamedTextColor.YELLOW))
        }
    }
}
