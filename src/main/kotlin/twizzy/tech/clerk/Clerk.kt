package twizzy.tech.clerk;

import com.google.inject.Inject
import com.velocitypowered.api.event.Subscribe
import com.velocitypowered.api.event.permission.PermissionsSetupEvent
import com.velocitypowered.api.event.proxy.ProxyInitializeEvent
import com.velocitypowered.api.permission.PermissionFunction
import com.velocitypowered.api.permission.PermissionProvider
import com.velocitypowered.api.permission.PermissionSubject
import com.velocitypowered.api.permission.Tristate
import com.velocitypowered.api.plugin.Plugin
import com.velocitypowered.api.proxy.Player
import com.velocitypowered.api.proxy.ProxyServer
import kotlinx.coroutines.*
import net.kyori.adventure.text.Component
import net.kyori.adventure.text.format.NamedTextColor
import net.kyori.adventure.text.logger.slf4j.ComponentLogger
import revxrsal.commands.autocomplete.SuggestionProvider
import revxrsal.commands.velocity.VelocityLamp
import revxrsal.commands.velocity.VelocityVisitors.brigadier
import twizzy.tech.clerk.commands.*
import twizzy.tech.clerk.player.Account
import twizzy.tech.clerk.player.Authentication
import twizzy.tech.clerk.player.ConnectionHandler
import twizzy.tech.clerk.player.Friends
import twizzy.tech.clerk.player.Ranks
import twizzy.tech.clerk.player.StaffManager
import twizzy.tech.clerk.util.JaSync
import twizzy.tech.clerk.util.Lettuce
import java.util.*

@Plugin(
    id = "clerk", name = "clerk", authors = ["mightbmax"], version = "2.0"
)

class Clerk @Inject constructor(val logger: ComponentLogger, val server: ProxyServer) {



    enum class MessageType {
        SUCCESS, ATTEMPT, ERROR
    }
    data class DatabaseMessage(val message: String, val type: MessageType)

    // Create a single JaSync instance for the entire plugin
    val jaSync = JaSync(this)
    val lettuce = Lettuce(this)

    // Use lazy initialization for components with dependencies
    val account by lazy { Account(this) }
    val ranks by lazy { Ranks(this) }
    val friends by lazy { Friends(this) }
    val manage by lazy { Manage(this) }
    val register by lazy { Register(this) }

    // Create our handler classes - also using lazy initialization to ensure dependencies are available
    val staffManager by lazy { StaffManager(this) }
    private val connectionHandler by lazy { ConnectionHandler(this) }
    private val authentication by lazy { Authentication(this) }


    private val lamp = VelocityLamp.builder(this, server)

        .suggestionProviders { providers ->

            providers.addProviderForAnnotation(ConnectionHandler.OnlinePlayers::class.java) { onlinePlayers: ConnectionHandler.OnlinePlayers ->
                SuggestionProvider { _ ->
                    runBlocking {
                        try {
                            // Get all online players from the server instead of Redis
                            val onlinePlayers = server.allPlayers
                                .filter { player ->
                                    // Only include players who are actually connected to a server (not in login/auth screen)
                                    player.currentServer.isPresent &&
                                    // Filter out vanished staff members
                                    !staffManager.vanishedStaff.containsKey(player.username)
                                }
                                .map { it.username }

                            // Return the filtered list of online player names
                            onlinePlayers
                        } catch (e: Exception) {
                            logger.warn(
                                Component.text(
                                    "Failed to get online players: ${e.message}",
                                    NamedTextColor.YELLOW
                                )
                            )
                            emptyList()
                        }
                    }
                }
            }


            providers.addProviderForAnnotation(Account.CachedAccounts::class.java) { cachedAccounts: Account.CachedAccounts ->
                // Cache for usernames to avoid repeated Redis calls
                val cachedUsernamesList = mutableListOf<String>()
                var lastCacheTime = 0L
                val cacheExpiryMs = 30000L // Cache expires after 30 seconds

                SuggestionProvider { _ ->
                    runBlocking {
                        try {
                            val currentTime = System.currentTimeMillis()

                            // Check if we need to refresh the cache
                            if (cachedUsernamesList.isEmpty() || currentTime - lastCacheTime > cacheExpiryMs) {

                                // Get the Redis connection synchronously to avoid dispatcher switching overhead for simple operations
                                val redisConn = lettuce.connection
                                if (redisConn == null) {
                                    logger.warn(Component.text("Redis connection is null, cannot get cached accounts", NamedTextColor.YELLOW))
                                    return@runBlocking emptyList()
                                }

                                // Use a limit to prevent retrieving too many keys (most commands only need a few suggestions)
                                val accountKeys = try {
                                    // Use keys command but limit the results we process
                                    withContext(Dispatchers.IO) {
                                        val allKeys = redisConn.sync().keys("account:*")
                                        // Limit to 100 results for better performance
                                        allKeys.take(100)
                                    }
                                } catch (e: Exception) {
                                    logger.warn(Component.text(
                                        "Failed to get cached accounts from Redis: ${e.message}",
                                        NamedTextColor.YELLOW
                                    ))
                                    emptyList()
                                }

                                // Extract usernames from keys
                                cachedUsernamesList.clear()
                                cachedUsernamesList.addAll(accountKeys.map { it.removePrefix("account:") })

                                // Update the cache timestamp
                                lastCacheTime = currentTime
                            }

                            // Return all cached usernames without filtering
                            cachedUsernamesList
                        } catch (e: Exception) {
                            logger.warn(Component.text(
                                "Error in cached accounts suggestion provider: ${e.message}",
                                NamedTextColor.YELLOW
                            ))
                            emptyList()
                        }
                    }
                }
            }

            providers.addProviderForAnnotation(Ranks.CachedRanks::class.java) { cachedRanks: Ranks.CachedRanks ->
                SuggestionProvider { _ ->
                    runBlocking {
                        try {
                            // Get all ranks from in-memory cache
                            val cachedRanks = ranks.getAllCachedRanks()

                            // Return a list of rank names
                            cachedRanks.keys.toList()
                        } catch (e: Exception) {
                            // Fallback to empty list if rank retrieval fails
                            logger.warn(
                                Component.text(
                                    "Failed to get cached ranks: ${e.message}",
                                    NamedTextColor.YELLOW
                                )
                            )
                            emptyList()
                        }
                    }
                }
            }

            // Add a new provider for player's friends list
            providers.addProviderForAnnotation(Friends.FriendList::class.java) { playerFriends: Friends.FriendList ->
                SuggestionProvider { context ->
                    runBlocking {
                        try {
                            // Get the command sender (player)
                            val sender = context.actor().asPlayer()
                            if (sender !is Player) {
                                return@runBlocking emptyList()
                            }

                            // Get the player's friends list
                            val friendsList = friends.getFriends(sender.username)

                            // Return the list of friend names
                            friendsList
                        } catch (e: Exception) {
                            logger.warn(
                                Component.text(
                                    "Failed to get player's friends: ${e.message}",
                                    NamedTextColor.YELLOW
                                )
                            )
                            emptyList()
                        }
                    }
                }
            }

            // Add a new provider for player's friend requests
            providers.addProviderForAnnotation(Friends.FriendRequests::class.java) { friendRequests: Friends.FriendRequests ->
                SuggestionProvider { context ->
                    runBlocking {
                        try {
                            // Get the command sender (player)
                            val sender = context.actor().asPlayer()
                            if (sender !is Player) {
                                return@runBlocking emptyList()
                            }

                            // Get the player's incoming friend requests
                            val requestsList = friends.getRequests(sender.username)

                            // Return the list of request sender names
                            requestsList
                        } catch (e: Exception) {
                            logger.warn(
                                Component.text(
                                    "Failed to get player's friend requests: ${e.message}",
                                    NamedTextColor.YELLOW
                                )
                            )
                            emptyList()
                        }
                    }
                }
            }
        }

        .build()
    private val scope = CoroutineScope(Dispatchers.IO)
    val unauthenticatedPlayers = mutableSetOf<UUID>()
    val awaitingPasswordConfirmation = mutableMapOf<Player, String>()

    init {
        logger.info(Component.text("Attempting to connect to the account database...", NamedTextColor.YELLOW))

        runBlocking {
            val dbMessages = jaSync.initializeDatabase()
            dbMessages.forEach { msg ->
                val color = when (msg.type) {
                    MessageType.SUCCESS -> NamedTextColor.GREEN
                    MessageType.ATTEMPT -> NamedTextColor.YELLOW
                    MessageType.ERROR -> NamedTextColor.RED
                }
                logger.info(Component.text(msg.message, color))
            }
            logger.info(Component.text("Successfully connected to the database!", NamedTextColor.GREEN))

            // Initialize Redis cache and log messages
            val cacheMessages = lettuce.initializeCache()
            cacheMessages.forEach { msg ->
                val color = when (msg.type) {
                    MessageType.SUCCESS -> NamedTextColor.GREEN
                    MessageType.ATTEMPT -> NamedTextColor.YELLOW
                    MessageType.ERROR -> NamedTextColor.RED
                }
                logger.info(Component.text(msg.message, color))
            }
            
            // Initialize Ranks system and log messages
            val ranksMessages = ranks.initialize()
            ranksMessages.forEach { msg ->
                val color = when (msg.type) {
                    MessageType.SUCCESS -> NamedTextColor.GREEN
                    MessageType.ATTEMPT -> NamedTextColor.YELLOW
                    MessageType.ERROR -> NamedTextColor.RED
                }
                logger.info(Component.text(msg.message, color))
            }
            
            // Start auto-sync after database connections are established
            startAutoSync()
        }

        // Register Commands
        lamp.register(Register(this))
        lamp.register(Login(this))
        lamp.register(Manage(this))
        lamp.register(Permission(this))
        lamp.register(Rank(this))
        lamp.register(Grant(this))
        lamp.register(LastSeen(this))
        lamp.register(Lobby(this))
        lamp.register(Friend(this))
        lamp.register(StaffChat(this))
        lamp.register(Vanish(this))

        lamp.register()

        lamp.accept(brigadier(server))
    }

    @Subscribe
    fun onProxyInitialization(event: ProxyInitializeEvent) {
        logger.info(Component.text("The plugin has finished setting up and is now running!", NamedTextColor.GREEN))


        // Register event handlers
        server.eventManager.register(this, staffManager)
        server.eventManager.register(this, connectionHandler)
        server.eventManager.register(this, authentication)
    }

    fun synchronize() {
        scope.launch {
            logger.info(Component.text("Starting Redis to PostgreSQL synchronization...", NamedTextColor.YELLOW))

            val startTime = System.currentTimeMillis()
            try {
                val syncCount = lettuce.synchronizeToPostgres(jaSync)
                val endTime = System.currentTimeMillis()
                val duration = (endTime - startTime) / 1000.0

                logger.info(Component.text(
                    "Successfully synchronized $syncCount accounts from Redis to PostgreSQL (took $duration seconds)",
                    NamedTextColor.GREEN
                ))
            } catch (e: Exception) {
                logger.error(Component.text(
                    "Failed to synchronize Redis to PostgreSQL: ${e.message}",
                    NamedTextColor.RED
                ))
            }
        }
    }


    private fun startAutoSync() {
        val config = jaSync.config
        val syncInterval = config.autoSync
        
        if (syncInterval.isBlank()) {
            logger.info(Component.text("Auto-sync is disabled (empty interval)", NamedTextColor.YELLOW))
            return
        }
        
        // Parse interval (format like "10m", "1h", etc.)
        val duration = parseDuration(syncInterval)
        if (duration <= 0) {
            logger.info(Component.text("Auto-sync is disabled (invalid interval: $syncInterval)", NamedTextColor.YELLOW))
            return
        }
        
        logger.info(Component.text("Starting auto-sync with interval: $syncInterval", NamedTextColor.GREEN))
        
        scope.launch {
            while (true) {
                delay(duration)
                try {
                    synchronize()
                } catch (e: Exception) {
                    logger.error(Component.text("Error during auto-sync: ${e.message}", NamedTextColor.RED))
                }
            }
        }
    }

    /**
     * Parses duration strings like "10m", "1h", "30s" into milliseconds
     */
    private fun parseDuration(duration: String): Long {
        val pattern = java.util.regex.Pattern.compile("(\\d+)([dhms])", java.util.regex.Pattern.CASE_INSENSITIVE)
        val matcher = pattern.matcher(duration)
        if (matcher.matches()) {
            val value = matcher.group(1)?.toLongOrNull() ?: return 0
            return when (matcher.group(2)?.lowercase()) {
                "d" -> value * 24 * 60 * 60 * 1000 // days to ms
                "h" -> value * 60 * 60 * 1000      // hours to ms
                "m" -> value * 60 * 1000           // minutes to ms
                "s" -> value * 1000                // seconds to ms
                else -> 0
            }
        }
        return 0
    }

    @Subscribe
    fun onClerkPermissions(event: PermissionsSetupEvent) {
        logger.info(Component.text("Successfully registered Clerk's permission provider.", NamedTextColor.GREEN))
        event.provider = object : PermissionProvider {
            override fun createFunction(subject: PermissionSubject): PermissionFunction {
                return PermissionFunction { permission ->
                    if (subject is Player) {
                        runBlocking {
                            // Find the username for this player
                            val username = subject.username

                            // Check for direct permission match first
                            if (account.checkPermission(username, permission, lettuce)) {
                                return@runBlocking Tristate.TRUE
                            }

                            // Check for wildcard permissions
                            // For example, if permission is "clerk.grant.add", check "clerk.*" and "clerk.grant.*"
                            val permParts = permission.split(".")
                            for (i in 1 until permParts.size) {
                                val wildcardPerm = permParts.subList(0, i).joinToString(".") + ".*"
                                if (account.checkPermission(username, wildcardPerm, lettuce)) {
                                    return@runBlocking Tristate.TRUE
                                }
                            }

                            Tristate.FALSE
                        }
                    } else {
                        Tristate.UNDEFINED
                    }
                }
            }
        }
    }

    // Clean up database connections when plugin is disabled
    fun shutdown() {
        jaSync.shutdown()
        lettuce.shutdown()
    }
}
