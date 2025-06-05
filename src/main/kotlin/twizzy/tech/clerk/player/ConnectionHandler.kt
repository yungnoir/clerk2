package twizzy.tech.clerk.player

import com.velocitypowered.api.event.Subscribe
import com.velocitypowered.api.event.connection.DisconnectEvent
import com.velocitypowered.api.event.player.PlayerChooseInitialServerEvent
import com.velocitypowered.api.proxy.Player
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import net.kyori.adventure.text.Component
import net.kyori.adventure.text.format.NamedTextColor
import twizzy.tech.clerk.Clerk
import java.util.concurrent.ConcurrentHashMap

class ConnectionHandler(private val clerk: Clerk) {

    @Target(AnnotationTarget.VALUE_PARAMETER)
    annotation class OnlinePlayers()

    private val logger = clerk.logger
    private val server = clerk.server
    private val jaSync = clerk.jaSync
    private val lettuce = clerk.lettuce
    private val staffManager = clerk.staffManager

    // Cache server instances to avoid repeated lookups
    private val serverCache = ConcurrentHashMap<String, com.velocitypowered.api.proxy.server.RegisteredServer>()

    // Batch update queue for last_seen updates
    private val lastSeenUpdateQueue = ConcurrentHashMap<String, String>() // username to serverName
    private val lastSeenBatchSize = 10
    private val lastSeenUpdateInterval = 5_000L // 5 seconds

    init {
        // Initialize server cache
        server.allServers.forEach { serverCache[it.serverInfo.name] = it }

        // Start batch processor for last_seen updates
        startLastSeenBatchProcessor()
    }

    private fun startLastSeenBatchProcessor() {
        clerk.scope.launch {
            while (true) {
                try {
                    processLastSeenUpdates()
                    delay(lastSeenUpdateInterval)
                } catch (e: Exception) {
                    logger.error(Component.text(
                        "Error in last_seen batch processor: ${e.message}",
                        NamedTextColor.RED
                    ))
                    delay(lastSeenUpdateInterval * 2) // Longer delay on error
                }
            }
        }
    }

    private suspend fun processLastSeenUpdates() {
        if (lastSeenUpdateQueue.isEmpty()) return

        // Take up to batchSize items
        val batch = lastSeenUpdateQueue.entries.take(lastSeenBatchSize)

        // Process the batch
        for ((username, serverName) in batch) {
            try {
                withContext(Dispatchers.IO) {
                    lettuce.updateLastSeen(username, serverName)
                }
                // Remove from queue after successful update
                lastSeenUpdateQueue.remove(username)
            } catch (e: Exception) {
                logger.warn(Component.text(
                    "Failed to update last_seen for $username: ${e.message}",
                    NamedTextColor.YELLOW
                ))
            }
        }
    }

    @Subscribe
    fun onProxyConnect(event: PlayerChooseInitialServerEvent) {
        val player = event.player
        val uuid = player.uniqueId

        if (!clerk.unauthenticatedPlayers.contains(uuid)) {
            val lobby = getServerCached("lobby")
            if (lobby != null) {
                event.setInitialServer(lobby)
                // Check if player has staff permission and add to staffManager
                clerk.scope.launch {
                    // Use the shared Account instance from Clerk instead of creating a new one
                    val hasPermission = clerk.account.checkPermission(player.username, "clerk.staff", clerk.lettuce)
                    if (hasPermission) {
                        staffManager.addStaff(player)
                    }
                }
            } else {
                logger.warn(Component.text(
                    "Lobby server not found, defaulting to auth.",
                    NamedTextColor.YELLOW
                ))
                val auth = getServerCached("auth")
                if (auth != null) {
                    event.setInitialServer(auth)
                }
            }
        } else {
            logger.info(
                Component.text(
                    "Player ${player.username} is not authenticated, keeping in auth server.",
                    NamedTextColor.YELLOW
                )
            )
            sendAuthReminder(player)
        }
    }

    private fun getServerCached(name: String): com.velocitypowered.api.proxy.server.RegisteredServer? {
        return serverCache.computeIfAbsent(name) { serverName ->
            server.getServer(serverName).orElse(null)
        }
    }

    @Subscribe(priority = 100)
    fun onProxyDisconnect(event: DisconnectEvent) {
        val player = event.player

        // Always update last_seen on disconnect for all authenticated players
        if (!clerk.unauthenticatedPlayers.contains(player.uniqueId)) {
            val username = player.username

            try {
                // Queue last_seen update
                lastSeenUpdateQueue[username] = "offline"

                // Asynchronously update their data from Redis to PostgreSQL
                clerk.scope.launch {
                    try {
                        logger.info(Component.text(
                            "Player $username disconnected, synchronizing their Redis cache to PostgreSQL...",
                            NamedTextColor.YELLOW
                        ))

                        // Sync only this specific player's data from Redis to PostgreSQL
                        val success = lettuce.synchronizePlayerToPostgres(username, jaSync)

                        if (success) {
                            logger.info(Component.text(
                                "Successfully synchronized $username's data to PostgreSQL.",
                                NamedTextColor.GREEN
                            ))
                        } else {
                            logger.warn(Component.text(
                                "No Redis cache found for $username or sync failed.",
                                NamedTextColor.YELLOW
                            ))
                        }
                    } catch (e: Exception) {
                        logger.error(Component.text(
                            "Failed to synchronize $username's cache to PostgreSQL: ${e.message}",
                            NamedTextColor.RED
                        ))
                    }
                }
            } catch (e: Exception) {
                logger.error(Component.text(
                    "Error processing disconnect for $username: ${e.message}",
                    NamedTextColor.RED
                ))
            }
        }
    }

    private fun sendAuthReminder(player: Player) {
        val uuid = player.uniqueId
        clerk.scope.launch {
            while (clerk.unauthenticatedPlayers.contains(uuid)) {
                player.sendActionBar(Component.text("Please /login or /register to access the server."))
                delay(2_000)
            }
        }
    }
}

