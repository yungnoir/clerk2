package twizzy.tech.clerk.util

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.lettuce.core.RedisClient
import io.lettuce.core.RedisURI
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.async.RedisAsyncCommands
import io.lettuce.core.api.sync.RedisCommands
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import net.kyori.adventure.text.Component
import net.kyori.adventure.text.format.NamedTextColor
import twizzy.tech.clerk.Clerk
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

class Lettuce(private val clerk: Clerk) {

    private val config = JacksonFactory.loadDatabaseConfig()
    private val redis = config.redis

    private var client: RedisClient? = null
    var connection: StatefulRedisConnection<String, String>? = null

    // Reusable Jackson mapper instance
    private val mapper = jacksonObjectMapper()

    // Cache connection commands for better performance
    var syncCommands: RedisCommands<String, String>? = null
    private var asyncCommands: RedisAsyncCommands<String, String>? = null

    // Local memory cache for frequent lookups (username -> account data)
    private val localCache = ConcurrentHashMap<String, Pair<String, Long>>() // JSON string and expiry timestamp
    private val LOCAL_CACHE_TTL_MS = 5_000L // 5 seconds local memory cache

    suspend fun initializeCache(): List<Clerk.DatabaseMessage> {
        val messages = mutableListOf<Clerk.DatabaseMessage>()
        return try {
            messages.add(Clerk.DatabaseMessage("Attempting to connect to the Redis cache...", Clerk.MessageType.ATTEMPT))
            val uri = RedisURI.Builder.redis(redis.host)
                .withPort(redis.port)
                .withAuthentication(redis.username, redis.password)
                .build()
            client = RedisClient.create(uri)
            connection = withContext(Dispatchers.IO) { client!!.connect() }

            // Initialize commands
            syncCommands = connection?.sync()
            asyncCommands = connection?.async()

            // Simple ping to check connection
            val pong = withContext(Dispatchers.IO) { asyncCommands!!.ping().get() }
            if (pong == "PONG") {
                messages.add(Clerk.DatabaseMessage("Successfully connected to the Lettuce cache!", Clerk.MessageType.SUCCESS))
            } else {
                messages.add(Clerk.DatabaseMessage("Connected to Lettuce but ping failed.", Clerk.MessageType.ERROR))
            }
            messages
        } catch (e: Exception) {
            messages.add(Clerk.DatabaseMessage("Error connecting to Lettuce: ${e.message}", Clerk.MessageType.ERROR))
            messages
        }
    }

    suspend fun cachePlayerAccount(username: String) {
        try {
            // Query relevant account information
            val query = """
                SELECT username, platform, registered_date, country, region, permissions, ranks, friends, settings, last_seen
                FROM accounts
                WHERE username = '${username.replace("'", "''")}'
                LIMIT 1
            """.trimIndent()
            val result = clerk.jaSync.executeQuery(query)
            val row = result.rows.firstOrNull() ?: return

            // Safely convert OffsetDateTime to ISO string if needed
            val registeredDateObj = row["registered_date"]
            val registeredDate = when (registeredDateObj) {
                is String -> registeredDateObj
                is java.time.OffsetDateTime -> registeredDateObj.toString()
                else -> registeredDateObj?.toString() ?: ""
            }

            // Ensure permissions is valid JSON
            val permissionsJson = row.getString("permissions") ?: "[]"
            val validPermissionsJson = try {
                // Validate by parsing and re-serializing
                val permissions = mapper.readValue(permissionsJson, Any::class.java)
                mapper.writeValueAsString(permissions)
            } catch (e: Exception) {
                // If parsing fails, default to empty array
                "[]"
            }

            // Get last_seen timestamp
            val lastSeenValue = row.get("last_seen")
            val lastSeenTimestamp = when (lastSeenValue) {
                is Number -> lastSeenValue.toLong()
                is String -> {
                    if (lastSeenValue.contains("-") && lastSeenValue.contains(":")) {
                        // Format like "2025-05-20 01:22:21.000 -0700"
                        val parts = lastSeenValue.split(" ")
                        val datePart = parts[0]
                        val timePart = if (parts.size > 1) parts[1] else "00:00:00"

                        try {
                            val timestamp = java.sql.Timestamp.valueOf("$datePart $timePart")
                            timestamp.time / 1000
                        } catch (e: Exception) {
                            try {
                                val instantStr = "${datePart}T$timePart".replace(" ", "")
                                java.time.Instant.parse(instantStr + "Z").epochSecond
                            } catch (e2: Exception) {
                                System.currentTimeMillis() / 1000 // Default to current time if parse fails
                            }
                        }
                    } else {
                        lastSeenValue.toLongOrNull() ?: (System.currentTimeMillis() / 1000)
                    }
                }
                is java.time.OffsetDateTime -> lastSeenValue.toEpochSecond()
                is java.sql.Timestamp -> lastSeenValue.time / 1000
                else -> System.currentTimeMillis() / 1000
            }

            // Get and enhance the friends list from PostgreSQL
            val friendsJson = row.getString("friends") ?: "[]"
            val friendsList = try {
                val list = mapper.readValue(friendsJson, List::class.java) as List<*>
                list.mapNotNull {
                    when (it) {
                        is String -> it
                        is Map<*, *> -> it["username"]?.toString()
                        else -> null
                    }
                }
            } catch (e: Exception) {
                clerk.logger.warn(Component.text("Error parsing friends list from PostgreSQL: ${e.message}", NamedTextColor.YELLOW))
                emptyList()
            }

            // Enhance friends list with last seen data immediately
            val enhancedFriendsList = if (friendsList.isNotEmpty()) {
                clerk.logger.info(Component.text(
                    "=== Building enhanced friends list for $username with ${friendsList.size} friends during account cache ===",
                    NamedTextColor.GOLD
                ))

                // Get last seen data for all friends
                val friendsWithLastSeen = friendsList.map { friendName ->
                    // Get last seen for each friend
                    val friendLastSeen = getLastSeenForFriend(friendName)

                    // Create friend data with both 'since' and 'lastseen'
                    val friendData = mutableMapOf<String, Any>(
                        "username" to friendName,
                        "lastseen" to (friendLastSeen ?: 0)
                    )

                    // Add 'since' from original friend data if available
                    val originalFriendData = extractSinceValueForFriend(friendsJson, friendName)
                    if (originalFriendData != null) {
                        friendData["since"] = originalFriendData
                    }

                    friendData
                }

                mapper.writeValueAsString(friendsWithLastSeen)
            } else {
                friendsJson
            }

            clerk.logger.info(Component.text(
                "Enhanced friends list for $username: $enhancedFriendsList",
                NamedTextColor.AQUA
            ))

            val accountInfo = mapOf(
                "username" to (row.getString("username") ?: ""),
                "platform" to (row.getString("platform") ?: ""),
                "registered_date" to registeredDate,
                "country" to (row.getString("country") ?: ""),
                "region" to (row.getString("region") ?: ""),
                "permissions" to validPermissionsJson,
                "ranks" to (row.getString("ranks") ?: "[]"),
                "friends" to enhancedFriendsList, // Use enhanced friends list
                "settings" to (row.getString("settings") ?: "{}"),
                "last_seen" to lastSeenTimestamp
                // Intentionally omitting incoming_requests and outgoing_requests
            )

            val json = mapper.writeValueAsString(accountInfo)

            // Cache in Redis with key "account:<username>" and set TTL to 1 hour (3600 seconds)
            withContext(Dispatchers.IO) {
                syncCommands?.setex("account:$username", 3600, json)
            }
            clerk.logger.info(
                Component.text(
                    "Cached account info for $username in Lettuce cache (TTL: 1 hour)",
                    NamedTextColor.GREEN
                )
            )
        } catch (e: Exception) {
            clerk.logger.error(
                Component.text(
                    "Failed to cache account info for $username: ${e.message}",
                    NamedTextColor.RED
                )
            )
        }
    }

    /**
     * Helper function to get last seen for a friend
     */
    private suspend fun getLastSeenForFriend(username: String): Long? {
        try {
            // First try to get from Redis cache
            val cachedData = getAccountCache(username)
            if (cachedData != null) {
                val accountData = mapper.readValue(cachedData, Map::class.java)
                if (accountData.containsKey("last_seen")) {
                    val lastSeen = accountData["last_seen"]
                    if (lastSeen is Number) {
                        return lastSeen.toLong()
                    }
                }
            }

            // If Redis unavailable or timestamp not found, try PostgreSQL
            val query = """
                SELECT last_seen FROM accounts
                WHERE LOWER(username) = LOWER('${username.replace("'", "''")}');
            """.trimIndent()

            val result = clerk.jaSync.executeQuery(query)
            if (result.rows.isEmpty()) {
                return null
            }

            val row = result.rows[0]
            val lastSeenValue = row.get("last_seen")

            return when (lastSeenValue) {
                is Number -> lastSeenValue.toLong()
                is String -> {
                    if (lastSeenValue.contains("-") && lastSeenValue.contains(":")) {
                        // Format like "2025-05-20 01:22:21.000 -0700"
                        val parts = lastSeenValue.split(" ")
                        val datePart = parts[0]
                        val timePart = if (parts.size > 1) parts[1] else "00:00:00"

                        try {
                            val timestamp = java.sql.Timestamp.valueOf("$datePart $timePart")
                            timestamp.time / 1000
                        } catch (e: Exception) {
                            try {
                                val timestampStr = "${datePart}T$timePart".replace(" ", "T")
                                java.time.Instant.parse(timestampStr + "Z").epochSecond
                            } catch (e2: Exception) {
                                null
                            }
                        }
                    } else {
                        lastSeenValue.toLongOrNull()
                    }
                }
                is java.time.OffsetDateTime -> lastSeenValue.toEpochSecond()
                is java.sql.Timestamp -> lastSeenValue.time / 1000
                else -> null
            }
        } catch (e: Exception) {
            clerk.logger.warn(Component.text("Error getting last seen for $username: ${e.message}", NamedTextColor.YELLOW))
            return null
        }
    }

    /**
     * Helper function to extract the 'since' value for a friend from original friends JSON
     */
    private fun extractSinceValueForFriend(friendsJson: String, friendName: String): Long? {
        try {
            val friendsList = mapper.readValue(friendsJson, List::class.java) as List<*>

            // Find the matching friend entry
            val friendEntry = friendsList.find { friend ->
                when (friend) {
                    is Map<*, *> -> (friend["username"]?.toString() ?: "").equals(friendName, ignoreCase = true)
                    is String -> friend.equals(friendName, ignoreCase = true)
                    else -> false
                }
            }

            // Extract since value if it exists
            return when (friendEntry) {
                is Map<*, *> -> {
                    val since = friendEntry["since"]
                    when (since) {
                        is Number -> since.toLong()
                        is String -> since.toLongOrNull()
                        else -> null
                    }
                }
                else -> null
            }
        } catch (e: Exception) {
            clerk.logger.warn(Component.text("Error extracting 'since' value: ${e.message}", NamedTextColor.YELLOW))
            return null
        }
    }

    /**
     * Gets the cached account JSON string for the given username, or null if not cached.
     */
    suspend fun getAccountCache(username: String): String? {
        val currentTime = System.currentTimeMillis()
        localCache[username]?.let { (cachedJson, expiry) ->
            if (currentTime < expiry) {
                return cachedJson
            }
        }

        return withContext(Dispatchers.IO) {
            syncCommands?.get("account:$username")?.also { json ->
                localCache[username] = json to (currentTime + LOCAL_CACHE_TTL_MS)
            }
        }
    }

    /**
     * Updates a player's ranks in the Redis cache
     * This is a direct update method that can be called from other classes
     */
    suspend fun updatePlayerRanks(username: String, ranksJson: String) {
        try {
            val cachedJson = getAccountCache(username) ?: return
            val accountData = mapper.readValue(cachedJson, MutableMap::class.java) as MutableMap<String, Any?>

            // Validate that ranksJson is valid JSON (array or object)
            try {
                mapper.readTree(ranksJson)
            } catch (e: Exception) {
                clerk.logger.warn(
                    Component.text(
                        "Invalid ranksJson provided for $username: ${e.message}",
                        NamedTextColor.YELLOW
                    )
                )
                return
            }
            // Store ranksJson as a string (double-encoded JSON)
            accountData["ranks"] = ranksJson

            val updatedJson = mapper.writeValueAsString(accountData)
            withContext(Dispatchers.IO) {
                syncCommands?.setex("account:$username", 3600, updatedJson)
            }

            clerk.logger.info(
                Component.text(
                    "Updated ranks for $username in Redis cache",
                    NamedTextColor.GREEN
                )
            )
        } catch (e: Exception) {
            clerk.logger.warn(
                Component.text(
                    "Failed to update ranks for $username in Redis: ${e.message}",
                    NamedTextColor.YELLOW
                )
            )
        }
    }

    /**
     * Updates the last_seen information for a player in Redis cache
     * @param username The player's username
     * @param serverName The name of the server the player is currently on (not stored)
     */
    suspend fun updateLastSeen(username: String, serverName: String) {
        try {
            val cachedJson = getAccountCache(username) ?: return

            // Parse the cached account data
            val accountData = mapper.readValue(cachedJson, MutableMap::class.java) as MutableMap<String, Any?>

            // Create last_seen data with only current timestamp
            val lastSeen = Instant.now().epochSecond

            // Update the last_seen field
            accountData["last_seen"] = lastSeen

            // Save back to Redis
            val updatedJson = mapper.writeValueAsString(accountData)
            withContext(Dispatchers.IO) {
                syncCommands?.setex("account:$username", 3600, updatedJson)
            }

            clerk.logger.info(
                Component.text(
                    "Updated last_seen for $username in Redis cache",
                    NamedTextColor.GREEN
                )
            )
        } catch (e: Exception) {
            clerk.logger.warn(
                Component.text(
                    "Failed to update last_seen for $username in Redis: ${e.message}",
                    NamedTextColor.YELLOW
                )
            )
        }
    }

    /**
     * Updates a player's friends list in Redis cache with last seen data
     * This is specifically for caching last_seen data and won't be synced to PostgreSQL
     */
    suspend fun updateFriendsCacheWithLastSeen(username: String, friendsJson: String) {
        clerk.logger.info(Component.text(
            "==== ENHANCING friends list with last seen data for $username ====",
            NamedTextColor.GOLD
        ))

        clerk.logger.info(Component.text(
            "Enhanced friends JSON: $friendsJson",
            NamedTextColor.AQUA
        ))

        try {
            val cachedJson = getAccountCache(username)
            if (cachedJson == null) {
                clerk.logger.warn(Component.text(
                    "No Redis cache entry found for $username - attempting to create new cache entry before updating friends",
                    NamedTextColor.YELLOW
                ))
                cachePlayerAccount(username)
                // Get the newly created cache entry
                val newCachedJson = getAccountCache(username)
                if (newCachedJson == null) {
                    clerk.logger.error(Component.text(
                        "Failed to create Redis cache for $username - cannot update friends list",
                        NamedTextColor.RED
                    ))
                    return
                }
                // Continue with the new cache
                updateCachedFriends(username, newCachedJson, friendsJson)
            } else {
                // Continue with existing cache
                updateCachedFriends(username, cachedJson, friendsJson)
            }
        } catch (e: Exception) {
            clerk.logger.error(Component.text(
                "Failed to update friends list with last seen for $username: ${e.message}\n${e.stackTraceToString().take(300)}",
                NamedTextColor.RED
            ))
        }
    }

    /**
     * Helper method to update cached friends in a consistent way
     */
    private suspend fun updateCachedFriends(username: String, cachedJson: String, friendsJson: String) {
        try {
            // Parse the cached account data
            val accountData = mapper.readValue(cachedJson, MutableMap::class.java) as MutableMap<String, Any?>

            // Log the current state before updating
            val beforeFriends = accountData["friends"]
            clerk.logger.info(Component.text(
                "Current friends data in Redis: $beforeFriends",
                NamedTextColor.AQUA
            ))

            // Update the friends list with enhanced data (including last_seen)
            accountData["friends"] = friendsJson

            // Save back to Redis
            val updatedJson = mapper.writeValueAsString(accountData)
            clerk.logger.info(Component.text(
                "Saving updated friends data to Redis for $username...",
                NamedTextColor.AQUA
            ))

            withContext(Dispatchers.IO) {
                syncCommands?.setex("account:$username", 3600, updatedJson)
            }

            // Verify the update worked by reading back from Redis
            val verifyJson = withContext(Dispatchers.IO) {
                syncCommands?.get("account:$username")
            }

            if (verifyJson != null) {
                try {
                    val verifyData = mapper.readValue(verifyJson, Map::class.java)
                    val afterFriends = verifyData["friends"]
                    clerk.logger.info(Component.text(
                        "Verified update - friends data now in Redis: $afterFriends",
                        NamedTextColor.GREEN
                    ))
                } catch (e: Exception) {
                    clerk.logger.warn(Component.text(
                        "Failed to verify Redis friends update for $username: ${e.message}",
                        NamedTextColor.YELLOW
                    ))
                }
            } else {
                clerk.logger.warn(Component.text(
                    "Failed to read back Redis data after update for $username",
                    NamedTextColor.YELLOW
                ))
            }

            // Clear local cache to ensure next get will fetch from Redis
            localCache.remove(username)

            clerk.logger.info(Component.text(
                "✅ Successfully updated enhanced friends list with last seen for $username in Redis cache",
                NamedTextColor.GREEN
            ))
        } catch (e: Exception) {
            clerk.logger.error(Component.text(
                "Error updating cached friends: ${e.message}",
                NamedTextColor.RED
            ))
        }
    }

    /**
     * Updates a player's friends list in Redis cache to match PostgreSQL
     * Call this whenever a friendship change happens in PostgreSQL
     */
    suspend fun updateFriendsListInRedis(username: String): Boolean {
        clerk.logger.debug(Component.text(
            "Starting updateFriendsListInRedis for $username",
            NamedTextColor.AQUA
        ))
        try {
            // Query the current friends from PostgreSQL
            clerk.logger.debug(Component.text(
                "Querying friends list from PostgreSQL for $username",
                NamedTextColor.AQUA
            ))
            val query = """
                SELECT friends FROM accounts
                WHERE LOWER(username) = LOWER('${username.replace("'", "''")}')
            """.trimIndent()

            val result = clerk.jaSync.executeQuery(query)
            if (result.rows.isEmpty()) {
                clerk.logger.warn(Component.text(
                    "User $username not found in PostgreSQL database",
                    NamedTextColor.YELLOW
                ))
                return false
            }

            val friendsJson = result.rows[0].getString("friends") ?: "[]"
            clerk.logger.debug(Component.text(
                "Retrieved friends for $username from PostgreSQL: $friendsJson",
                NamedTextColor.AQUA
            ))

            // Get the current Redis cache
            clerk.logger.debug(Component.text(
                "Retrieving existing cache for $username from Redis",
                NamedTextColor.AQUA
            ))
            val cachedJson = getAccountCache(username)
            if (cachedJson == null) {
                clerk.logger.warn(Component.text(
                    "No Redis cache entry found for $username, attempting to create a new one",
                    NamedTextColor.YELLOW
                ))
                // Try to cache the full account first
                cachePlayerAccount(username)
                clerk.logger.debug(Component.text(
                    "Created new Redis cache entry for $username",
                    NamedTextColor.GREEN
                ))
                return true
            }

            clerk.logger.debug(Component.text(
                "Found Redis cache for $username, preparing to update",
                NamedTextColor.AQUA
            ))

            val accountData = try {
                mapper.readValue(cachedJson, MutableMap::class.java) as MutableMap<String, Any?>
            } catch (e: Exception) {
                clerk.logger.error(Component.text(
                    "Failed to parse Redis cache JSON for $username: ${e.message}\n${e.stackTraceToString().take(300)}",
                    NamedTextColor.RED
                ))
                return false
            }

            // Debug log before update
            val beforeFriends = accountData["friends"]
            clerk.logger.debug(Component.text(
                "Current friends in Redis for $username: $beforeFriends",
                NamedTextColor.AQUA
            ))

            // Update the friends list
            accountData["friends"] = friendsJson

            // Debug log after update
            clerk.logger.debug(Component.text(
                "Updated friends in memory to: $friendsJson",
                NamedTextColor.AQUA
            ))

            // Save back to Redis with updated friends
            val updatedJson = mapper.writeValueAsString(accountData)
            clerk.logger.debug(Component.text(
                "Saving updated cache to Redis for $username",
                NamedTextColor.AQUA
            ))

            withContext(Dispatchers.IO) {
                try {
                    val result = syncCommands?.setex("account:$username", 3600, updatedJson)
                    clerk.logger.debug(Component.text(
                        "Redis setex result: $result",
                        NamedTextColor.AQUA
                    ))
                } catch (e: Exception) {
                    clerk.logger.error(Component.text(
                        "Redis setex command failed: ${e.message}\n${e.stackTraceToString().take(300)}",
                        NamedTextColor.RED
                    ))
                    throw e
                }
            }

            // Validate the update by reading it back from Redis
            val verifyJson = withContext(Dispatchers.IO) {
                syncCommands?.get("account:$username")
            }

            if (verifyJson != null) {
                try {
                    val verifyData = mapper.readValue(verifyJson, MutableMap::class.java)
                    val verifyFriends = verifyData["friends"]
                    clerk.logger.debug(Component.text(
                        "Verified Redis update for $username - friends: $verifyFriends",
                        NamedTextColor.GREEN
                    ))

                    // Check if the data matches what we tried to save
                    if (verifyFriends != friendsJson && verifyFriends.toString() != friendsJson) {
                        clerk.logger.warn(Component.text(
                            "Redis update verification warning: saved and retrieved values don't match exactly",
                            NamedTextColor.YELLOW
                        ))
                    }
                } catch (e: Exception) {
                    clerk.logger.warn(Component.text(
                        "Failed to parse verification data: ${e.message}",
                        NamedTextColor.YELLOW
                    ))
                }
            } else {
                clerk.logger.warn(Component.text(
                    "Failed to verify Redis update - could not retrieve data after save",
                    NamedTextColor.YELLOW
                ))
            }

            clerk.logger.info(Component.text(
                "Successfully updated friends list for $username in Redis to match PostgreSQL",
                NamedTextColor.GREEN
            ))

            return true
        } catch (e: Exception) {
            clerk.logger.error(Component.text(
                "Failed to update Redis friends list for $username: ${e.message}\n${e.stackTraceToString().take(300)}",
                NamedTextColor.RED
            ))
            return false
        }
    }

    /**
     * Synchronizes Redis cache data to PostgreSQL database
     * Returns the number of accounts synchronized
     */
    suspend fun synchronizeToPostgres(jaSync: JaSync): Int {
        if (connection == null) {
            throw IllegalStateException("Redis connection is not established")
        }
        
        // Get all account keys from Redis
        val keys = withContext(Dispatchers.IO) {
            syncCommands?.keys("account:*") ?: emptyList()
        }
        
        if (keys.isEmpty()) {
            return 0
        }
        
        var syncCount = 0
        for (key in keys) {
            val username = key.removePrefix("account:")
            val json = withContext(Dispatchers.IO) {
                syncCommands?.get(key)
            } ?: continue
            
            try {
                val accountData = mapper.readValue(json, Map::class.java)
                
                // Build SQL update parts
                val updateParts = mutableListOf<String>()
                
                // Add each field that should be synced
                if (accountData.containsKey("permissions")) {
                    val permsValue = accountData["permissions"]
                    val permsJson = formatValueAsJson(permsValue)
                    updateParts.add("permissions = '$permsJson'::jsonb")
                }
                
                if (accountData.containsKey("ranks")) {
                    val ranksValue = accountData["ranks"]
                    val ranksJson = formatValueAsJson(ranksValue)
                    updateParts.add("ranks = '$ranksJson'::jsonb") 
                }

                if (accountData.containsKey("settings")) {
                    val settingsValue = accountData["settings"]
                    val settingsJson = formatValueAsJson(settingsValue)
                    updateParts.add("settings = '$settingsJson'::jsonb")
                }
                
                // Skip if no fields to update
                if (updateParts.isEmpty()) {
                    continue
                }
                
                // Execute update
                val query = """
                    UPDATE accounts 
                    SET ${updateParts.joinToString(", ")}
                    WHERE username = '${username.replace("'", "''")}'
                """.trimIndent()
                
                jaSync.executeQuery(query)
                syncCount++
            } catch (e: Exception) {
                clerk.logger.warn(
                    Component.text(
                        "Failed to sync account $username: ${e.message}\n${e.stackTraceToString().take(500)}",
                        NamedTextColor.YELLOW
                    )
                )
            }
        }
        
        return syncCount
    }

    /**
     * Synchronizes a single player's Redis cache data to PostgreSQL
     * @param username The player's username
     * @param jaSync The JaSync instance to use for database operations
     * @return true if sync was successful, false if player not in cache or sync failed
     */
    suspend fun synchronizePlayerToPostgres(username: String, jaSync: JaSync): Boolean {
        if (connection == null) {
            throw IllegalStateException("Redis connection is not established")
        }
        
        // Get the player's cache from Redis
        val key = "account:$username"
        val json = withContext(Dispatchers.IO) {
            syncCommands?.get(key)
        } ?: return false
        
        try {
            val accountData = mapper.readValue(json, Map::class.java)
            
            // Build SQL update parts
            val updateParts = mutableListOf<String>()
            
            // Add each field that should be synced
            if (accountData.containsKey("permissions")) {
                val permsValue = accountData["permissions"]
                val permsJson = formatValueAsJson(permsValue)
                updateParts.add("permissions = '$permsJson'::jsonb")
            }
            
            if (accountData.containsKey("ranks")) {
                val ranksValue = accountData["ranks"]
                val ranksJson = formatValueAsJson(ranksValue)
                updateParts.add("ranks = '$ranksJson'::jsonb") 
            }


            if (accountData.containsKey("settings")) {
                val settingsValue = accountData["settings"]
                val settingsJson = formatValueAsJson(settingsValue)
                updateParts.add("settings = '$settingsJson'::jsonb")
            }
            
            // Skip if no fields to update
            if (updateParts.isEmpty()) {
                return false
            }
            
            // Execute update
            val query = """
                UPDATE accounts 
                SET ${updateParts.joinToString(", ")}
                WHERE LOWER(username) = LOWER('${username.replace("'", "''")}')
            """.trimIndent()
            
            jaSync.executeQuery(query)
            return true
        } catch (e: Exception) {
            clerk.logger.warn(
                Component.text(
                    "Failed to sync account $username: ${e.message}",
                    NamedTextColor.YELLOW
                )
            )
            return false
        }
    }
    
    /**
     * Helper function to format various values as valid JSON strings
     */
    private fun formatValueAsJson(value: Any?): String {
        return when {
            value == null -> "[]"
            value is String -> {
                if (value.isBlank()) {
                    "[]"
                } else if ((value.startsWith("[") && value.endsWith("]")) || 
                          (value.startsWith("{") && value.endsWith("}"))) {
                    // It's already JSON formatted, validate it
                    validateJsonString(value)
                } else {
                    // It's a raw string, convert to JSON array with quoted string
                    validateJsonString("[\"${value.replace("\"", "\\\"")}\"]")
                }
            }
            value is Collection<*> -> {
                // Convert collection to proper JSON array
                validateJsonString(mapper.writeValueAsString(value))
            }
            else -> {
                // For any other type, try to convert to JSON
                validateJsonString(mapper.writeValueAsString(listOf(value.toString())))
            }
        }
    }

    /**
     * Validates and ensures a string is proper JSON
     * Returns a valid JSON string that can be safely used in PostgreSQL
     */
    private fun validateJsonString(jsonStr: String): String {
        return try {
            // Parse and re-serialize to ensure valid JSON
            val jsonNode = mapper.readTree(jsonStr)
            mapper.writeValueAsString(jsonNode)
        } catch (e: Exception) {
            // If parsing fails, try to interpret as a simple value
            try {
                if (jsonStr.startsWith("[") && !jsonStr.contains("\"") && !jsonStr.contains(":")) {
                    // Handle arrays of unquoted strings like [test1, test2]
                    val items = jsonStr.trim('[', ']').split(",").map { it.trim() }
                    mapper.writeValueAsString(items)
                } else if (!jsonStr.startsWith("[") && !jsonStr.startsWith("{")) {
                    // Handle plain text that's not already in JSON format
                    // This handles cases like "permission1" or permission1
                    // Convert single plain text to a proper JSON array with quoted strings
                    mapper.writeValueAsString(listOf(jsonStr))
                } else {
                    // For malformed JSON that still starts with brackets
                    try {
                        // Try to fix common issues
                        val fixedJson = fixMalformedJson(jsonStr)
                        mapper.writeValueAsString(mapper.readValue(fixedJson, List::class.java))
                    } catch (e3: Exception) {
                        // Default to an empty array as last resort
                        "[]"
                    }
                }
            } catch (e2: Exception) {
                clerk.logger.warn(Component.text(
                    "Could not parse JSON value: $jsonStr - Error: ${e2.message}",
                    NamedTextColor.YELLOW
                ))
                // Fallback to empty array for any unparseable value
                "[]"
            }
        }
    }

    /**
     * Attempts to fix common JSON formatting issues
     */
    private fun fixMalformedJson(jsonStr: String): String {
        // Case 1: Handle unquoted keys and values in arrays like [permission1, permission2]
        if (jsonStr.startsWith("[") && jsonStr.endsWith("]")) {
            val content = jsonStr.substring(1, jsonStr.length - 1)
            val items = content.split(",").map { it.trim() }
            val quotedItems = items.map { 
                if (it.startsWith("\"") && it.endsWith("\"")) {
                    it // Already quoted
                } else {
                    "\"${it.replace("\"", "\\\"")}\""  // Add quotes
                }
            }
            return "[${quotedItems.joinToString(", ")}]"
        }
        
        // Case 2: Handle JSON-like object with unquoted keys like {permission: value}
        // This is more complex and would require regex parsing
        
        return jsonStr // Return original if no simple fixes apply
    }

    /**
     * Helper method to extract just the usernames from enhanced friends list
     * This ensures we don't save last_seen data to PostgreSQL
     */
    private fun extractBaseFriendsList(friendsData: Any?): List<Map<String, Any>> {
        // Default empty list if friendsData is null
        if (friendsData == null) return emptyList()

        try {
            val friendsList = when (friendsData) {
                is String -> {
                    if (friendsData.startsWith("[")) {
                        mapper.readValue(friendsData, List::class.java) as List<*>
                    } else {
                        listOf(friendsData)
                    }
                }
                is List<*> -> friendsData
                else -> listOf(friendsData.toString())
            }

            // Transform the enhanced list to remove last_seen data
            return friendsList.mapNotNull { friend ->
                when (friend) {
                    is String -> mapOf("username" to friend)
                    is Map<*, *> -> {
                        val username = friend["username"]?.toString() ?: return@mapNotNull null
                        val since = friend["since"]?.toString()?.toLongOrNull()

                        // Only keep username and 'since' if it exists - explicitly exclude 'lastseen'
                        if (since != null) {
                            mapOf("username" to username, "since" to since)
                        } else {
                            mapOf("username" to username)
                        }
                    }
                    else -> null
                }
            }
        } catch (e: Exception) {
            clerk.logger.warn(Component.text("Error extracting base friends list: ${e.message}", NamedTextColor.YELLOW))
            return emptyList()
        }
    }

    fun shutdown() {
        connection?.close()
        client?.shutdown()
    }

    // Redis message channel
    // We will send a pubsub message to notify other servers of changes
    private val RANKS_UPDATE_CHANNEL = "clerk:rank_updates"
    private val ACCOUNT_UPDATE_CHANNEL = "clerk:account_updates"

    /**
     * Notifies all connected servers to check the database for updates
     * This is a simple notification without specific data
     */
    suspend fun notifyRanksUpdate() {
        if (connection == null || syncCommands == null) {
            clerk.logger.warn(
                Component.text(
                    "Cannot send database update notification: Redis connection not established",
                    NamedTextColor.YELLOW
                )
            )
            return
        }

        try {
            // Create a simple notification message
            val notification = mapOf(
                "type" to "database_update",
                "timestamp" to Instant.now().epochSecond
            )

            // Convert to JSON
            val message = mapper.writeValueAsString(notification)

            // Send to Redis PubSub channel
            withContext(Dispatchers.IO) {
                val result = syncCommands?.publish(RANKS_UPDATE_CHANNEL, message)
                clerk.logger.info(
                    Component.text(
                        "Sent database update notification to $result servers",
                        NamedTextColor.GREEN
                    )
                )
            }
        } catch (e: Exception) {
            clerk.logger.error(
                Component.text(
                    "Failed to publish database update notification: ${e.message}",
                    NamedTextColor.RED
                )
            )
        }
    }

    /**
     * Notifies all connected servers to check for an account update for a specific username
     * Sends a message to the ACCOUNT_UPDATE_CHANNEL with the username
     */
    suspend fun notifyAccountUpdate(username: String) {
        if (connection == null || syncCommands == null) {
            clerk.logger.warn(
                Component.text(
                    "Cannot send account update notification: Redis connection not established",
                    NamedTextColor.YELLOW
                )
            )
            return
        }

        try {
            // Create a notification message with username
            val notification = mapOf(
                "type" to "account_update",
                "username" to username,
                "timestamp" to Instant.now().epochSecond
            )

            // Convert to JSON
            val message = mapper.writeValueAsString(notification)

            // Send to Redis PubSub channel
            withContext(Dispatchers.IO) {
                val result = syncCommands?.publish(ACCOUNT_UPDATE_CHANNEL, message)
                clerk.logger.info(
                    Component.text(
                        "Sent account update notification for $username to $result servers",
                        NamedTextColor.GREEN
                    )
                )
            }
        } catch (e: Exception) {
            clerk.logger.error(
                Component.text(
                    "Failed to publish account update notification for $username: ${e.message}",
                    NamedTextColor.RED
                )
            )
        }
    }
}
