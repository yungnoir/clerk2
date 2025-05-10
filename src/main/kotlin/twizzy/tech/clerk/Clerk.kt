package twizzy.tech.clerk;

import com.google.inject.Inject
import com.velocitypowered.api.event.Subscribe
import com.velocitypowered.api.event.command.CommandExecuteEvent
import com.velocitypowered.api.event.player.GameProfileRequestEvent
import com.velocitypowered.api.event.player.PlayerChatEvent
import com.velocitypowered.api.event.player.ServerPreConnectEvent
import com.velocitypowered.api.event.proxy.ProxyInitializeEvent
import com.velocitypowered.api.plugin.Plugin
import com.velocitypowered.api.proxy.Player
import com.velocitypowered.api.proxy.ProxyServer
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import twizzy.tech.clerk.util.JaSync
import twizzy.tech.clerk.util.JaSync.MessageType
import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.format.NamedTextColor;
import net.kyori.adventure.text.logger.slf4j.ComponentLogger
import org.geysermc.floodgate.api.FloodgateApi
import revxrsal.commands.velocity.VelocityLamp
import revxrsal.commands.velocity.VelocityVisitors.brigadier
import twizzy.tech.clerk.commands.Login
import twizzy.tech.clerk.commands.Manage
import twizzy.tech.clerk.commands.Register
import twizzy.tech.clerk.player.Account

@Plugin(
    id = "clerk", name = "clerk", authors = ["mightbmax"], version = BuildConstants.VERSION
)
class Clerk @Inject constructor(val logger: ComponentLogger, val server: ProxyServer) {

    // Create a single JaSync instance for the entire plugin
    val jaSync = JaSync()
    private val lamp = VelocityLamp.builder(this, server).build()
    private val scope = CoroutineScope(Dispatchers.IO)
    val unauthenticatedPlayers = mutableSetOf<Player>()
    val awaitingPasswordConfirmation = mutableMapOf<Player, String>()
    
    // Pass the jaSync instance to the commands
    private val registerCommand = Register(this, jaSync)
    private val loginCommand = Login(this, jaSync)
    private val manageCommand = Manage(this, jaSync)

    init {
        logger.info(Component.text("Attempting to connect to the account database...", NamedTextColor.YELLOW))
        
        runBlocking { 
            val messages = jaSync.initializeDatabase()
            messages.forEach { msg ->
                val color = when (msg.type) {
                    MessageType.SUCCESS -> NamedTextColor.GREEN
                    MessageType.ATTEMPT -> NamedTextColor.YELLOW
                    MessageType.ERROR -> NamedTextColor.RED
                }
                logger.info(Component.text(msg.message, color))
            }
            logger.info(Component.text("Successfully connected to the database!", NamedTextColor.GREEN))
        }

        // Register Commands
        lamp.register(registerCommand)
        lamp.register(loginCommand)
        lamp.register(manageCommand)
        lamp.accept(brigadier(server))
    }

    @Subscribe
    fun onProxyInitialization(event: ProxyInitializeEvent) {
        logger.info(Component.text("The plugin has finished setting up and is now running!", NamedTextColor.GREEN))
    }

    @Subscribe
    fun onProxyConnect(event: ServerPreConnectEvent) {
        val player = event.player
        val isBedrock = FloodgateApi.getInstance().isFloodgatePlayer(player.uniqueId)
        val id = if (isBedrock) {
            FloodgateApi.getInstance().getPlayer(player.uniqueId)?.xuid ?: player.uniqueId.toString()
        } else {
            player.uniqueId.toString()
        }

        val ip = player.remoteAddress.address.hostAddress

        runBlocking {
            val username = Account(jaSync, logger).getLastLoginUsername(id, ip)
            var autoLock = false
            var recentLogin = false
            if (username != null) {
                // Check if auto_lock is enabled for this account
                val autoLockQuery = """
                    SELECT auto_lock, logins FROM accounts WHERE username = '${username.replace("'", "''")}' LIMIT 1;
                """.trimIndent()
                val result = jaSync.executeQuery(autoLockQuery)
                autoLock = result.rows.firstOrNull()?.getBoolean("auto_lock") ?: false

                // If auto_lock is enabled, check if last login was within 2 minutes
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
                                val loginTime = java.time.OffsetDateTime.parse(dateStr)
                                val now = java.time.OffsetDateTime.now()
                                val duration = java.time.Duration.between(loginTime, now)
                                if (duration.toMinutes() < 2) {
                                    recentLogin = true
                                }
                            }
                        }
                    } catch (_: Exception) {}
                }
            }
            if (username != null && (!autoLock || recentLogin)) {
                // Auto-authenticated, send to lobby if not already going there
                unauthenticatedPlayers.remove(player)
                val lobby = server.getServer("lobby")
                if (lobby.isPresent && event.originalServer.serverInfo.name != "lobby") {
                    event.setResult(ServerPreConnectEvent.ServerResult.allowed(lobby.get()))
                }
            } else if (username != null && autoLock && !recentLogin) {
                logger.info(Component.text("Player ${player.username} would have been logged in, but auto-lock is enabled.", NamedTextColor.YELLOW))
                unauthenticatedPlayers.add(player)
                sendAuthReminder(player)
                val auth = server.getServer("auth")
                if (auth.isPresent && event.originalServer.serverInfo.name != "auth") {
                    event.setResult(ServerPreConnectEvent.ServerResult.allowed(auth.get()))
                }
            } else {
                // Not authenticated, keep in auth server
                unauthenticatedPlayers.add(player)
                sendAuthReminder(player)
                val auth = server.getServer("auth")
                if (auth.isPresent && event.originalServer.serverInfo.name != "auth") {
                    event.setResult(ServerPreConnectEvent.ServerResult.allowed(auth.get()))
                }
            }
        }
    }

    @Subscribe
    fun gameProfile(event: GameProfileRequestEvent) {
        val address = event.connection.remoteAddress.address.hostAddress
        val isBedrock = FloodgateApi.getInstance().isFloodgatePlayer(event.gameProfile.id)
        val id = if (isBedrock) {
            FloodgateApi.getInstance().getPlayer(event.gameProfile.id)?.xuid ?: event.gameProfile.id.toString()
        } else {
            event.gameProfile.id.toString()
        }
        runBlocking {
            val username = Account(jaSync, logger).getLastLoginUsername(id, address)
            if (username != null) {
                event.gameProfile = event.originalProfile.withName(username)
                logger.info(Component.text("${event.originalProfile.name} has joined logged in as $username.", NamedTextColor.GREEN))
            }
        }
    }

    private fun sendAuthReminder(player: Player) {
        scope.launch {
            while (unauthenticatedPlayers.contains(player)) {
                // Send reminder as action bar instead of chat
                player.sendActionBar(Component.text("Please /login or /register to access the server."))
                delay(2_000)
            }
        }
    }

    @Subscribe
    fun unAuthCommand(event: CommandExecuteEvent) {
        val player = event.commandSource
        if (unauthenticatedPlayers.contains(player)) {
            if (event.command.contains("register") || event.command.contains("login")) {
                return
            } else {
                event.result = CommandExecuteEvent.CommandResult.denied()
            }
        }
    }

    @Subscribe
    fun unAuthChat(event: PlayerChatEvent) {
        val player = event.player
        // Always deny chat event if in registration or password reset process
        if (unauthenticatedPlayers.contains(player)) {
            val expectedPassword = awaitingPasswordConfirmation[player]
            if (expectedPassword != null) {
                event.result = PlayerChatEvent.ChatResult.denied() // Prevent message from being broadcast
                if (event.message == expectedPassword) {
                    awaitingPasswordConfirmation.remove(player)
                    repeat(100) { player.sendMessage(Component.text("")) }
                    registerCommand.confirmRegistration(player, event.message)
                } else {
                    // Clear registration state if passwords do not match
                    awaitingPasswordConfirmation.remove(player)
                    registerCommand.confirmRegistration(player, event.message)
                    logger.info(Component.text("Player ${player.username} failed registration due to mismatched passwords.", NamedTextColor.RED))
                }
                return
            } else {
                event.result = PlayerChatEvent.ChatResult.denied()
                return
            }
        }
        // Handle password reset chat if in reset process
        // Always deny chat event if in password reset process
        val manageClass = manageCommand
        val pendingField = manageClass.javaClass.getDeclaredField("pendingPasswordResets")
        pendingField.isAccessible = true
        val pendingResets = pendingField.get(manageClass) as MutableMap<*, *>
        if (pendingResets.containsKey(player)) {
            event.result = PlayerChatEvent.ChatResult.denied()
            manageCommand.handlePasswordResetChat(player, event.message)
        }
    }
    
    // Clean up database connections when plugin is disabled
    fun shutdown() {
        jaSync.shutdown()
    }
}
