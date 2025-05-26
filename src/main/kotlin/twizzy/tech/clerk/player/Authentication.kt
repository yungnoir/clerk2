package twizzy.tech.clerk.player

import com.velocitypowered.api.event.Subscribe
import com.velocitypowered.api.event.command.CommandExecuteEvent
import com.velocitypowered.api.event.player.GameProfileRequestEvent
import com.velocitypowered.api.event.player.PlayerChatEvent
import com.velocitypowered.api.proxy.Player
import kotlinx.coroutines.runBlocking
import net.kyori.adventure.text.Component
import net.kyori.adventure.text.format.NamedTextColor
import org.geysermc.floodgate.api.FloodgateApi
import twizzy.tech.clerk.Clerk
import twizzy.tech.clerk.commands.Manage
import twizzy.tech.clerk.commands.Register
import twizzy.tech.clerk.util.JaSync
import twizzy.tech.clerk.util.Lettuce
import java.lang.reflect.Field
import java.time.Duration
import java.time.OffsetDateTime

class Authentication(private val clerk: Clerk) {
    private val logger = clerk.logger
    private val jaSync = clerk.jaSync
    private val lettuce = clerk.lettuce
    private val account = clerk.account // Use the shared account instance from Clerk

    @Subscribe
    fun gameProfile(event: GameProfileRequestEvent) {
        val address = event.connection.remoteAddress.address.hostAddress
        val isBedrock = FloodgateApi.getInstance().isFloodgatePlayer(event.gameProfile.id)
        val id = if (isBedrock) {
            FloodgateApi.getInstance().getPlayer(event.gameProfile.id)?.xuid ?: event.gameProfile.id
        } else {
            event.gameProfile.id
        }
        val uuid = event.gameProfile.id
        val ip = address

        runBlocking {
            val username = account.getLastLoginUsername(id.toString(), address)
            if (username == null) {
                clerk.unauthenticatedPlayers.add(uuid)
                return@runBlocking
            }

            // Account found, check auto_lock
            val autoLockQuery = """
            SELECT auto_lock, logins FROM accounts WHERE username = '${username.replace("'", "''")}' LIMIT 1;
            """.trimIndent()
            val result = jaSync.executeQuery(autoLockQuery)
            val autoLock = result.rows.firstOrNull()?.getBoolean("auto_lock") ?: false

            if (autoLock) {
                val loginsJson = result.rows.firstOrNull()?.getString("logins") ?: "[]"
                try {
                    val arr = org.json.JSONArray(loginsJson)
                    val lastLogin = (arr.length() - 1 downTo 0)
                        .map { arr.getJSONObject(it) }
                        .firstOrNull {
                            (it.optString("id") == id || it.optString("ip_address") == ip)
                        }
                    if (lastLogin != null) {
                        val dateStr = lastLogin.optString("date", null)
                        if (dateStr != null) {
                            val loginTime = OffsetDateTime.parse(dateStr)
                            val now = OffsetDateTime.now()
                            val duration = Duration.between(loginTime, now)
                            if (duration.toMinutes() >= 2) {
                                clerk.unauthenticatedPlayers.add(uuid)
                                logger.info(Component.text("Player ${event.originalProfile.name} would have been logged in, but auto-lock is enabled and last login is too old.", NamedTextColor.YELLOW))
                                return@runBlocking
                            }
                        }
                    } else {
                        clerk.unauthenticatedPlayers.add(uuid)
                        logger.info(Component.text("Player ${event.originalProfile.name} has no recent login, auto-lock enabled.", NamedTextColor.YELLOW))
                        return@runBlocking
                    }
                } catch (_: Exception) {
                    clerk.unauthenticatedPlayers.add(uuid)
                    logger.info(Component.text("Player ${event.originalProfile.name} login check failed, auto-lock enabled.", NamedTextColor.YELLOW))
                    return@runBlocking
                }
            }

            // Account exists and either auto_lock is off or recent login is within 2 minutes
            clerk.unauthenticatedPlayers.remove(uuid)
            event.gameProfile = event.originalProfile.withName(username)

            // Ensure account is in Redis cache, but don't force an overwrite if it exists
            lettuce.cachePlayerAccount(username)
            logger.info(Component.text("${event.originalProfile.name} has joined logged in as $username.", NamedTextColor.GREEN))
        }
    }

    @Subscribe
    fun unAuthCommand(event: CommandExecuteEvent) {
        val player = event.commandSource as? Player ?: return
        val uuid = player.uniqueId
        if (clerk.unauthenticatedPlayers.contains(uuid)) {
            if (event.command.contains("register") || event.command.contains("login")) {
                return
            } else {
                event.result = CommandExecuteEvent.CommandResult.denied()
            }
        }
    }

    @Subscribe
    fun unAuthChat(event: PlayerChatEvent) {
        val player = event.player as? Player ?: return
        val uuid = player.uniqueId
        if (clerk.unauthenticatedPlayers.contains(uuid)) {
            val expectedPassword = clerk.awaitingPasswordConfirmation[player]
            if (expectedPassword != null) {
                event.result = PlayerChatEvent.ChatResult.denied()
                // Create only one instance of Register and use it
                val register = clerk.register
                if (event.message == expectedPassword) {
                    clerk.awaitingPasswordConfirmation.remove(player)
                    register.confirmRegistration(player, event.message)
                } else {
                    clerk.awaitingPasswordConfirmation.remove(player)
                    register.confirmRegistration(player, event.message)
                }
                return
            } else {
                event.result = PlayerChatEvent.ChatResult.denied()
                return
            }
        }
    }

    @Subscribe
    fun resetChat(event: PlayerChatEvent) {
        val player = event.player

        // Check if player is in password reset process
        if (Manage.pendingPasswordResets.containsKey(player)) {
            // Deny the chat message from being sent to global chat
            event.result = PlayerChatEvent.ChatResult.denied()

            // Pass the chat message to the password reset handler
            val manage = clerk.manage
            manage.handlePasswordResetChat(player, event.message)
        }
    }
}
