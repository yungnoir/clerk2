package twizzy.tech.clerk.player

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import net.kyori.adventure.text.Component
import net.kyori.adventure.text.format.NamedTextColor
import net.kyori.adventure.text.logger.slf4j.ComponentLogger
import twizzy.tech.clerk.Clerk
import twizzy.tech.clerk.util.JaSync
import twizzy.tech.clerk.util.Lettuce
import java.time.Instant

/**
 * Friends system implementation using PostgreSQL for storage with optional Redis caching
 */
class Friends(private val clerk: Clerk) {


    // Define an annotation for friend list suggestions
    @Target(AnnotationTarget.VALUE_PARAMETER)
    annotation class FriendList

    // Define an annotation for friend list suggestions
    @Target(AnnotationTarget.VALUE_PARAMETER)
    annotation class FriendRequests

    private val jaSync = clerk.jaSync
    private val logger = clerk.logger
    private val lettuce = clerk.lettuce
    private val account = clerk.account

    private val mapper = jacksonObjectMapper()

    /**
     * Send a friend request from sender to target or confirm friendship if reciprocal
     * @param sender username of request sender
     * @param target username of request target
     * @return FriendActionResult indicating the result of the operation
     */
    suspend fun addFriend(sender: String, target: String): FriendActionResult {
        logger.info(Component.text(
            "Processing friend request from $sender to $target",
            NamedTextColor.GOLD
        ))

        // Don't allow friending yourself
        if (sender.equals(target, ignoreCase = true)) {
            logger.info(Component.text("User $sender attempted to friend themselves", NamedTextColor.YELLOW))
            return FriendActionResult.SELF_REQUEST
        }

        // Check if target exists
        if (!userExists(target)) {
            logger.info(Component.text("User $sender tried to friend non-existent user $target", NamedTextColor.YELLOW))
            return FriendActionResult.USER_NOT_FOUND
        }

        // Check if target has toggled requests off
        if (account.getSetting(target, "toggledRequests", false, lettuce) as? Boolean == true) {
            logger.info(Component.text("User $sender tried to friend $target who has disabled requests", NamedTextColor.YELLOW))
            return FriendActionResult.REQUESTS_DISABLED
        }

        // Check if they're already friends
        if (areFriends(sender, target)) {
            logger.info(Component.text("Users $sender and $target are already friends", NamedTextColor.YELLOW))
            return FriendActionResult.ALREADY_FRIENDS
        }

        // Check if sender already sent a request to target
        if (hasOutgoingRequest(sender, target)) {
            logger.info(Component.text("User $sender already sent a request to $target", NamedTextColor.YELLOW))
            return FriendActionResult.REQUEST_ALREADY_SENT
        }

        // Check if target already sent a request to sender (if so, make them friends)
        if (hasIncomingRequest(sender, target)) {
            logger.info(Component.text(
                "User $sender accepted $target's friend request - making them friends",
                NamedTextColor.GREEN
            ))

            // Make them friends (reciprocal connection)
            val friendshipSuccess = addFriendship(sender, target)
            logger.info(Component.text(
                "Friendship creation result: " + (if (friendshipSuccess) "SUCCESS" else "FAILED"),
                if (friendshipSuccess) NamedTextColor.GREEN else NamedTextColor.RED
            ))

            removeRequest(target, sender) // Remove the existing request


            lettuce.notifyAccountUpdate(target)
            lettuce.notifyAccountUpdate(sender)
            return FriendActionResult.NOW_FRIENDS
        }

        // Otherwise, create a new request
        logger.info(Component.text("Creating new friend request from $sender to $target", NamedTextColor.AQUA))
        createRequest(sender, target)
        return FriendActionResult.REQUEST_SENT
    }

    /**
     * Deny/reject a friend request from sender to target
     * @param sender username who sent the original request
     * @param target username who received the request and is now denying it
     * @return boolean indicating if request was successfully denied
     */
    suspend fun denyFriend(sender: String, target: String): Boolean {
        // Check if there's an incoming request to deny
        if (!hasIncomingRequest(target, sender)) {
            return false
        }

        // Remove the request
        return removeRequest(sender, target)
    }

    /**
     * Remove a friend or cancel a sent friend request
     * @param sender username initiating the removal
     * @param target username to remove from friends or cancel request to
     * @return FriendActionResult indicating the result of the operation
     */
    suspend fun removeFriend(sender: String, target: String): FriendActionResult {
        // Check if they are friends
        if (areFriends(sender, target)) {
            // Remove the friendship (both ways)
            removeFriendship(sender, target)
            return FriendActionResult.FRIEND_REMOVED
        }

        // Check if sender has a pending outgoing request to target
        if (hasOutgoingRequest(sender, target)) {
            // Cancel the request
            removeRequest(sender, target)
            return FriendActionResult.REQUEST_CANCELLED
        }

        return FriendActionResult.NOT_FRIENDS
    }

    /**
     * Get the list of friends for a user
     * @param username the user to get friends for
     * @return List of friend usernames
     */
    suspend fun getFriends(username: String): List<String> {
        logger.info(Component.text(
            "==== Getting friends list for $username ====",
            NamedTextColor.GOLD
        ))

        // First check Redis cache - Redis should always be available
        try {
            logger.info(Component.text(
                "Attempting to get friends from Redis cache for $username",
                NamedTextColor.AQUA
            ))
            val cachedData = lettuce.getAccountCache(username)
            if (cachedData != null) {
                logger.info(Component.text(
                    "Found Redis cache entry for $username with data length: ${cachedData.length}",
                    NamedTextColor.GREEN
                ))
                val accountData = mapper.readValue(cachedData, Map::class.java)
                val friendsData = accountData["friends"]
                if (friendsData != null) {
                    logger.info(Component.text(
                        "Friends data in Redis: $friendsData (type: ${friendsData.javaClass.name})",
                        NamedTextColor.AQUA
                    ))
                    // Parse the enhanced friendlist from Redis
                    val parsedFriends = parseEnhancedFriends(friendsData)
                    logger.info(Component.text(
                        "Parsed ${parsedFriends.size} friends from Redis cache for $username: $parsedFriends",
                        NamedTextColor.GREEN
                    ))
                    return parsedFriends
                } else {
                    logger.info(Component.text(
                        "Redis cache entry exists for $username but 'friends' key is null",
                        NamedTextColor.YELLOW
                    ))
                }
            } else {
                logger.info(Component.text(
                    "No Redis cache entry found for $username",
                    NamedTextColor.YELLOW
                ))
            }
        } catch (e: Exception) {
            logger.warn(Component.text(
                "Error reading friends from Redis: ${e.message}\n${e.stackTraceToString().take(300)}",
                NamedTextColor.RED
            ))
        }

        logger.info(Component.text(
            "Falling back to PostgreSQL for friends list for $username",
            NamedTextColor.YELLOW
        ))

        // If cache miss or error, get from PostgreSQL
        val query = """
            SELECT friends FROM accounts
            WHERE LOWER(username) = LOWER('${username.replace("'", "''")}');
        """.trimIndent()

        val result = jaSync.executeQuery(query)
        if (result.rows.isEmpty()) {
            logger.warn(Component.text(
                "User $username not found in PostgreSQL database",
                NamedTextColor.RED
            ))
            return emptyList()
        }

        val row = result.rows[0]
        val friendsJson = row.getString("friends") ?: "[]"
        logger.info(Component.text(
            "Retrieved friends from PostgreSQL for $username: $friendsJson",
            NamedTextColor.AQUA
        ))

        val friendsList = try {
            val parsedList = parseNames(friendsJson)
            logger.info(Component.text(
                "Parsed ${parsedList.size} friends from PostgreSQL for $username: $parsedList",
                NamedTextColor.GREEN
            ))
            parsedList
        } catch (e: Exception) {
            logger.error(Component.text(
                "Error parsing friends from PostgreSQL: ${e.message}\n${e.stackTraceToString().take(300)}",
                NamedTextColor.RED
            ))
            emptyList()
        }

        // When loading from PostgreSQL, trigger a cache refresh but don't wait for it
        if (friendsList.isNotEmpty()) {
            try {
                logger.info(Component.text(
                    "Triggering cache refresh for $username after PostgreSQL lookup",
                    NamedTextColor.AQUA
                ))
                // This will update the Redis cache with enhanced data
                lettuce.cachePlayerAccount(username)
            } catch (e: Exception) {
                logger.error(Component.text(
                    "Failed to trigger cache refresh: ${e.message}",
                    NamedTextColor.RED
                ))
            }
        }

        return friendsList
    }

    /**
     * Get incoming friend requests for a user
     * @param username the user to get requests for
     * @return List of usernames who sent requests
     */
    suspend fun getRequests(username: String): List<String> {
        val query = """
            SELECT incoming_requests FROM accounts
            WHERE LOWER(username) = LOWER('${username.replace("'", "''")}');
        """.trimIndent()

        val result = jaSync.executeQuery(query)
        if (result.rows.isEmpty()) {
            return emptyList()
        }

        val row = result.rows[0]
        val requestsJson = row.getString("incoming_requests") ?: "[]"

        return try {
            parseRequestSenders(requestsJson)
        } catch (e: Exception) {
            logger.warn(Component.text("Error parsing requests: ${e.message}", NamedTextColor.RED))
            emptyList()
        }
    }

    /**
     * Get outgoing friend requests for a user
     * @param username the user to get outgoing requests for
     * @return List of usernames who received requests
     */
    suspend fun getOutgoingRequests(username: String): List<String> {
        val query = """
            SELECT outgoing_requests FROM accounts
            WHERE LOWER(username) = LOWER('${username.replace("'", "''")}');
        """.trimIndent()

        val result = jaSync.executeQuery(query)
        if (result.rows.isEmpty()) {
            return emptyList()
        }

        val row = result.rows[0]
        val requestsJson = row.getString("outgoing_requests") ?: "[]"

        return try {
            parseRequestRecipients(requestsJson)
        } catch (e: Exception) {
            logger.warn(Component.text("Error parsing outgoing requests: ${e.message}", NamedTextColor.RED))
            emptyList()
        }
    }

    /**
     * Get the last seen timestamp for a player
     * First checks Redis cache, then falls back to PostgreSQL
     *
     * @param username the player to check
     * @return the last seen timestamp in epoch seconds, or null if not found
     */
    suspend fun getLastSeen(username: String): Long? {
        // First try to get from Redis cache if available
        if (lettuce != null) {
            try {
                val cachedData = lettuce.getAccountCache(username)
                if (cachedData != null) {
                    val accountData = mapper.readValue(cachedData, Map::class.java)
                    if (accountData.containsKey("last_seen")) {
                        val lastSeen = accountData["last_seen"]
                        if (lastSeen is Number) {
                            return lastSeen.toLong()
                        }
                    }
                }
            } catch (e: Exception) {
                logger.warn(Component.text("Error reading last_seen from Redis: ${e.message}", NamedTextColor.YELLOW))
            }
        }

        // If Redis unavailable or timestamp not found, try PostgreSQL
        val query = """
            SELECT last_seen FROM accounts
            WHERE LOWER(username) = LOWER('${username.replace("'", "''")}');
        """.trimIndent()

        val result = jaSync.executeQuery(query)
        if (result.rows.isEmpty()) {
            return null
        }

        val row = result.rows[0]
        val lastSeenValue = row.get("last_seen")

        return try {
            when (lastSeenValue) {
                is Number -> lastSeenValue.toLong()
                is String -> {
                    if (lastSeenValue.contains("-") && lastSeenValue.contains(":")) {
                        // Format like "2025-05-20 01:22:21.000 -0700"
                        val parts = lastSeenValue.split(" ")
                        val datePart = parts[0] // 2025-05-20
                        val timePart = if (parts.size > 1) parts[1] else "00:00:00" // 01:22:21.000

                        try {
                            // Try java.sql.Timestamp first
                            val timestamp = java.sql.Timestamp.valueOf("$datePart $timePart")
                            timestamp.time / 1000
                        } catch (e: Exception) {
                            // Try Instant parsing as fallback
                            val timestampStr = "${datePart}T${timePart}".replace(" ", "T")
                            try {
                                val instant = Instant.parse(timestampStr + "Z")
                                instant.epochSecond
                            } catch (e2: Exception) {
                                logger.warn(Component.text("Failed to parse last_seen: $lastSeenValue", NamedTextColor.YELLOW))
                                null
                            }
                        }
                    } else {
                        // Already in epoch seconds
                        lastSeenValue.toLongOrNull()
                    }
                }
                is java.time.OffsetDateTime -> lastSeenValue.toEpochSecond()
                is java.sql.Timestamp -> lastSeenValue.time / 1000
                else -> null
            }
        } catch (e: Exception) {
            logger.warn(Component.text("Error parsing last_seen: ${e.message}", NamedTextColor.YELLOW))
            null
        }
    }

    /**
     * Format a timestamp into a readable duration string
     * @param timestamp the epoch seconds timestamp
     * @param useAgo whether to append "ago" to the formatted string
     * @return formatted duration string like "2d 3h ago" or "5m ago"
     */
    fun formatLastSeen(timestamp: Long?, useAgo: Boolean = true): String {
        if (timestamp == null) return "Unknown"

        val now = Instant.now().epochSecond
        val seconds = now - timestamp

        if (seconds < 0) return "just now"

        val minutes = seconds / 60
        val hours = minutes / 60
        val days = hours / 24

        val formatted = when {
            days > 0 -> "${days}d ${hours % 24}h"
            hours > 0 -> "${hours}h ${minutes % 60}m"
            minutes > 0 -> "${minutes}m"
            else -> "just now"
        }

        return if (useAgo && formatted != "just now") "$formatted ago" else formatted
    }

    /**
     * Parse friends list from Redis that includes last seen data
     */
    private fun parseEnhancedFriends(friendsData: Any?): List<String> {
        if (friendsData == null) return emptyList()

        try {
            val friendsList = when (friendsData) {
                is String -> mapper.readValue(friendsData, List::class.java) as? List<*>
                is List<*> -> friendsData
                else -> null
            } ?: return emptyList()

            return friendsList.mapNotNull { friend ->
                when (friend) {
                    is String -> friend
                    is Map<*, *> -> friend["username"]?.toString()
                    else -> null
                }
            }
        } catch (e: Exception) {
            logger.warn(Component.text(
                "Error parsing enhanced friends list: ${e.message}",
                NamedTextColor.YELLOW
            ))
            return emptyList()
        }
    }

    /**
     * Check if two users are already friends
     */
    private suspend fun areFriends(user1: String, user2: String): Boolean {
        val query = """
            SELECT friends FROM accounts
            WHERE LOWER(username) = LOWER('${user1.replace("'", "''")}');
        """.trimIndent()

        val result = jaSync.executeQuery(query)
        if (result.rows.isEmpty()) {
            return false
        }

        val row = result.rows[0]
        val friendsJson = row.getString("friends") ?: "[]"

        val friends = parseNames(friendsJson)
        return friends.any { it.equals(user2, ignoreCase = true) }
    }

    /**
     * Check if user1 has sent a request to user2
     */
    private suspend fun hasOutgoingRequest(user1: String, user2: String): Boolean {
        val query = """
            SELECT outgoing_requests FROM accounts
            WHERE LOWER(username) = LOWER('${user1.replace("'", "''")}');
        """.trimIndent()

        val result = jaSync.executeQuery(query)
        if (result.rows.isEmpty()) {
            return false
        }

        val row = result.rows[0]
        val requestsJson = row.getString("outgoing_requests") ?: "[]"

        val requests = parseRequestRecipients(requestsJson)
        return requests.any { it.equals(user2, ignoreCase = true) }
    }

    /**
     * Check if user1 has received a request from user2
     */
    private suspend fun hasIncomingRequest(user1: String, user2: String): Boolean {
        val query = """
            SELECT incoming_requests FROM accounts
            WHERE LOWER(username) = LOWER('${user1.replace("'", "''")}');
        """.trimIndent()

        val result = jaSync.executeQuery(query)
        if (result.rows.isEmpty()) {
            return false
        }

        val row = result.rows[0]
        val requestsJson = row.getString("incoming_requests") ?: "[]"

        val requests = parseRequestSenders(requestsJson)
        return requests.any { it.equals(user2, ignoreCase = true) }
    }

    /**
     * Create a friend request from sender to target
     */
    private suspend fun createRequest(sender: String, target: String): Boolean {
        val timestamp = Instant.now().epochSecond

        val requests = listOf(
            // Add to sender's outgoing requests
            """
            UPDATE accounts
            SET outgoing_requests = outgoing_requests || jsonb_build_object(
                'to', '${target.replace("'", "''")}',
                'timestamp', $timestamp
            )
            WHERE LOWER(username) = LOWER('${sender.replace("'", "''")}');
            """.trimIndent(),

            // Add to target's incoming requests
            """
            UPDATE accounts
            SET incoming_requests = incoming_requests || jsonb_build_object(
                'from', '${sender.replace("'", "''")}',
                'timestamp', $timestamp
            )
            WHERE LOWER(username) = LOWER('${target.replace("'", "''")}');
            """.trimIndent()
        )

        try {
            jaSync.executeBatch(requests)
            return true
        } catch (e: Exception) {
            logger.warn(Component.text("Error creating request: ${e.message}", NamedTextColor.RED))
            return false
        }
    }

    /**
     * Remove a friend request from sender to target
     */
    private suspend fun removeRequest(sender: String, target: String): Boolean {
        val requests = listOf(
            // Remove from sender's outgoing requests
            """
            UPDATE accounts
            SET outgoing_requests = (
                SELECT COALESCE(jsonb_agg(req), '[]'::jsonb)
                FROM jsonb_array_elements(COALESCE(outgoing_requests, '[]'::jsonb)) AS req
                WHERE req->>'to' != '${target.replace("'", "''")}'
            )
            WHERE LOWER(username) = LOWER('${sender.replace("'", "''")}');
            """.trimIndent(),

            // Remove from target's incoming requests
            """
            UPDATE accounts
            SET incoming_requests = (
                SELECT COALESCE(jsonb_agg(req), '[]'::jsonb)
                FROM jsonb_array_elements(COALESCE(incoming_requests, '[]'::jsonb)) AS req
                WHERE req->>'from' != '${sender.replace("'", "''")}'
            )
            WHERE LOWER(username) = LOWER('${target.replace("'", "''")}');
            """.trimIndent()
        )

        try {
            jaSync.executeBatch(requests)
            return true
        } catch (e: Exception) {
            logger.warn(Component.text("Error removing request: ${e.message}", NamedTextColor.RED))
            return false
        }
    }

    /**
     * Add users as mutual friends
     */
    private suspend fun addFriendship(user1: String, user2: String): Boolean {
        val timestamp = Instant.now().epochSecond

        logger.info(Component.text(
            "Adding friendship between $user1 and $user2 with timestamp $timestamp",
            NamedTextColor.AQUA
        ))

        val queries = listOf(
            // Add user2 to user1's friends
            """
            UPDATE accounts
            SET friends = (
                CASE
                    WHEN NOT (COALESCE(friends, '[]'::jsonb) @> jsonb_build_array(jsonb_build_object('username', '${user2.replace("'", "''")}', 'since', $timestamp)))
                    THEN COALESCE(friends, '[]'::jsonb) || jsonb_build_object('username', '${user2.replace("'", "''")}', 'since', $timestamp)
                    ELSE COALESCE(friends, '[]'::jsonb)
                END
            )
            WHERE LOWER(username) = LOWER('${user1.replace("'", "''")}');
            """.trimIndent(),

            // Add user1 to user2's friends
            """
            UPDATE accounts
            SET friends = (
                CASE
                    WHEN NOT (COALESCE(friends, '[]'::jsonb) @> jsonb_build_array(jsonb_build_object('username', '${user1.replace("'", "''")}', 'since', $timestamp)))
                    THEN COALESCE(friends, '[]'::jsonb) || jsonb_build_object('username', '${user1.replace("'", "''")}', 'since', $timestamp)
                    ELSE COALESCE(friends, '[]'::jsonb)
                END
            )
            WHERE LOWER(username) = LOWER('${user2.replace("'", "''")}');
            """.trimIndent()
        )

        try {
            logger.info(Component.text(
                "Executing PostgreSQL query batch for friendship...",
                NamedTextColor.AQUA
            ))
            jaSync.executeBatch(queries)
            logger.info(Component.text(
                "PostgreSQL friendship update successful",
                NamedTextColor.GREEN
            ))

            // Force Redis cache update regardless of lettuce reference
            try {
                // We know at least one user must be online to have initiated this request
                logger.info(Component.text(
                    "Starting Redis cache update for both users...",
                    NamedTextColor.AQUA
                ))

                // Get a reference to clerk for redis access
                val clerk = jaSync.clerk

                if (clerk.lettuce != null) {
                    logger.info(Component.text(
                        "Redis IS available, proceeding with cache update",
                        NamedTextColor.GREEN
                    ))

                    // Get last seen data for both users to create enhanced entries
                    val user1LastSeen = getLastSeen(user1) ?: timestamp
                    val user2LastSeen = getLastSeen(user2) ?: timestamp

                    // Create enhanced friend entries with both 'since' and 'lastseen'
                    val user1FriendEntry = mapOf(
                        "username" to user2,
                        "since" to timestamp,
                        "lastseen" to user2LastSeen
                    )

                    val user2FriendEntry = mapOf(
                        "username" to user1,
                        "since" to timestamp,
                        "lastseen" to user1LastSeen
                    )

                    // Update user1's friends list in Redis with enhanced data
                    logger.info(Component.text(
                        "Updating Redis cache for user1: $user1 with enhanced friend data",
                        NamedTextColor.AQUA
                    ))
                    val user1Success = updateUserFriendCache(user1, user2, user1FriendEntry, clerk.lettuce)
                    logger.info(Component.text(
                        "Enhanced Redis update for $user1 " + (if (user1Success) "succeeded" else "failed"),
                        if (user1Success) NamedTextColor.GREEN else NamedTextColor.RED
                    ))

                    // Update user2's friends list in Redis
                    logger.info(Component.text(
                        "Updating Redis cache for user2: $user2 with enhanced friend data",
                        NamedTextColor.AQUA
                    ))
                    val user2Success = updateUserFriendCache(user2, user1, user2FriendEntry, clerk.lettuce)
                    logger.info(Component.text(
                        "Enhanced Redis update for $user2 " + (if (user2Success) "succeeded" else "failed"),
                        if (user2Success) NamedTextColor.GREEN else NamedTextColor.RED
                    ))

                    logger.info(Component.text(
                        "Updated Redis cache with new friendship between $user1 and $user2 with last seen data",
                        NamedTextColor.GREEN
                    ))
                } else {
                    // This is a fallback in case Redis isn't available through lettuce
                    // Try to check if any online player has this username to cache via their session
                    val onlineUser1 = clerk.server.getPlayer(user1).orElse(null)
                    val onlineUser2 = clerk.server.getPlayer(user2).orElse(null)

                    logger.warn(Component.text(
                        "Direct Redis reference unavailable. Attempting alternative caching method.",
                        NamedTextColor.YELLOW
                    ))

                    if (onlineUser1 != null || onlineUser2 != null) {
                        logger.info(Component.text(
                            "At least one user is online, manual cache update will occur on next data refresh",
                            NamedTextColor.AQUA
                        ))
                    } else {
                        logger.warn(Component.text(
                            "Neither user is currently online. Redis cache will update when they next login.",
                            NamedTextColor.YELLOW
                        ))
                    }
                }
            } catch (e: Exception) {
                logger.warn(Component.text(
                    "Failed to update Redis cache for friendship: ${e.message}\n${e.stackTraceToString().take(300)}",
                    NamedTextColor.YELLOW
                ))
                // Continue execution - the database was updated successfully even if Redis failed
            }

            return true
        } catch (e: Exception) {
            logger.warn(Component.text(
                "Error adding friendship: ${e.message}\n${e.stackTraceToString().take(300)}",
                NamedTextColor.RED
            ))
            return false
        }
    }

    /**
     * Update a user's Redis cache with enhanced friend data
     */
    private suspend fun updateUserFriendCache(username: String, friendName: String, enhancedEntry: Map<String, Any>, lettuce: Lettuce): Boolean {
        try {
            // Get current cache
            val cachedData = lettuce.getAccountCache(username)
            if (cachedData == null) {
                // No cache exists yet, create a new one
                lettuce.cachePlayerAccount(username)
                return true // The full cache refresh will have the updated friend data
            }

            // Parse existing cache
            val accountData = mapper.readValue(cachedData, Map::class.java) as MutableMap<String, Any?>

            // Get existing friends list
            val currentFriends = when (val existingFriends = accountData["friends"]) {
                is String -> {
                    try {
                        mapper.readValue(existingFriends, List::class.java) as MutableList<Any>
                    } catch (e: Exception) {
                        mutableListOf<Any>()
                    }
                }
                is List<*> -> existingFriends as MutableList<Any>
                else -> mutableListOf<Any>()
            }

            // Remove any existing entries for this friend
            val updatedFriends = currentFriends.filter { friend ->
                when (friend) {
                    is String -> !friend.equals(friendName, ignoreCase = true)
                    is Map<*, *> -> {
                        val friendUsername = friend["username"]?.toString() ?: ""
                        !friendUsername.equals(friendName, ignoreCase = true)
                    }
                    else -> true
                }
            }.toMutableList()

            // Add the enhanced entry with last seen data
            updatedFriends.add(enhancedEntry)

            // Update the friends list in the account data
            accountData["friends"] = updatedFriends

            // Save back to Redis
            val updatedJson = mapper.writeValueAsString(accountData)
            withContext(Dispatchers.IO) {
                lettuce.syncCommands?.setex("account:$username", 3600, updatedJson)
            }

            return true
        } catch (e: Exception) {
            logger.error(Component.text(
                "Error updating Redis cache for $username: ${e.message}",
                NamedTextColor.RED
            ))
            return false
        }
    }

    /**
     * Remove users as mutual friends
     */
    private suspend fun removeFriendship(user1: String, user2: String): Boolean {
        val queries = listOf(
            // Remove user2 from user1's friends
            """
            UPDATE accounts
            SET friends = (
                SELECT COALESCE(jsonb_agg(friend), '[]'::jsonb)
                FROM jsonb_array_elements(friends) AS friend
                WHERE friend->>'username' != '${user2.replace("'", "''")}'
            )
            WHERE LOWER(username) = LOWER('${user1.replace("'", "''")}');
            """.trimIndent(),

            // Remove user1 from user2's friends
            """
            UPDATE accounts
            SET friends = (
                SELECT COALESCE(jsonb_agg(friend), '[]'::jsonb)
                FROM jsonb_array_elements(friends) AS friend
                WHERE friend->>'username' != '${user1.replace("'", "''")}'
            )
            WHERE LOWER(username) = LOWER('${user2.replace("'", "''")}');
            """.trimIndent()
        )

        try {
            logger.info(Component.text(
                "Executing friendship removal for $user1 and $user2 in PostgreSQL",
                NamedTextColor.AQUA
            ))
            jaSync.executeBatch(queries)
            logger.info(Component.text(
                "PostgreSQL friendship removal successful",
                NamedTextColor.GREEN
            ))

            // Update Redis cache if available
            try {
                logger.info(Component.text(
                    "Updating Redis cache for friendship removal between $user1 and $user2",
                    NamedTextColor.AQUA
                ))

                if (lettuce != null) {
                    logger.info(Component.text(
                        "Redis is available, updating cache...",
                        NamedTextColor.AQUA
                    ))

                    // Integrated approach for both users - update and enhance in a single step
                    for (username in listOf(user1, user2)) {
                        try {
                            // Get updated friends list from PostgreSQL
                            val query = """
                                SELECT friends FROM accounts
                                WHERE LOWER(username) = LOWER('${username.replace("'", "''")}')
                            """.trimIndent()

                            val result = jaSync.executeQuery(query)
                            if (result.rows.isEmpty()) {
                                logger.warn(Component.text(
                                    "Could not find user $username for Redis cache update",
                                    NamedTextColor.YELLOW
                                ))
                                continue
                            }

                            val friendsJson = result.rows[0].getString("friends") ?: "[]"
                            logger.info(Component.text(
                                "Retrieved updated friends list from PostgreSQL for $username after removal: $friendsJson",
                                NamedTextColor.AQUA
                            ))

                            // Parse friends list from PostgreSQL
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
                                logger.warn(Component.text(
                                    "Error parsing friends list from PostgreSQL: ${e.message}",
                                    NamedTextColor.YELLOW
                                ))
                                continue
                            }

                            if (friendsList.isEmpty()) {
                                logger.info(Component.text(
                                    "User $username has no friends left after removal",
                                    NamedTextColor.AQUA
                                ))
                                // Update Redis with empty friends list
                                try {
                                    val cachedData = lettuce.getAccountCache(username)
                                    if (cachedData != null) {
                                        val accountData = mapper.readValue(cachedData, Map::class.java) as MutableMap<String, Any?>
                                        accountData["friends"] = "[]"
                                        val updatedJson = mapper.writeValueAsString(accountData)
                                        withContext(Dispatchers.IO) {
                                            lettuce.syncCommands?.setex("account:$username", 3600, updatedJson)
                                        }
                                        logger.info(Component.text(
                                            "Updated Redis cache for $username with empty friends list",
                                            NamedTextColor.GREEN
                                        ))
                                    }
                                } catch (e: Exception) {
                                    logger.warn(Component.text(
                                        "Failed to update Redis cache for $username with empty list: ${e.message}",
                                        NamedTextColor.YELLOW
                                    ))
                                }
                                continue
                            }

                            // Build enhanced friends list with last seen data directly
                            val enhancedFriendsList = friendsList.map { friendName ->
                                // Get last seen for each friend
                                val friendLastSeen = getLastSeen(friendName) ?: Instant.now().epochSecond

                                // Extract 'since' value from the original PostgreSQL data
                                val sinceValue = try {
                                    val friendsList = mapper.readValue(friendsJson, List::class.java) as List<*>
                                    val entry = friendsList.find { f ->
                                        when (f) {
                                            is Map<*, *> -> (f["username"]?.toString() ?: "").equals(friendName, ignoreCase = true)
                                            is String -> f.equals(friendName, ignoreCase = true)
                                            else -> false
                                        }
                                    }

                                    when (entry) {
                                        is Map<*, *> -> {
                                            val since = entry["since"]
                                            when (since) {
                                                is Number -> since.toLong()
                                                is String -> since.toLongOrNull()
                                                else -> null
                                            }
                                        }
                                        else -> null
                                    }
                                } catch (e: Exception) {
                                    null
                                }

                                // Create friend data with both 'username', 'since' and 'lastseen'
                                val friendData = mutableMapOf<String, Any>(
                                    "username" to friendName,
                                    "lastseen" to friendLastSeen
                                )
                                if (sinceValue != null) {
                                    friendData["since"] = sinceValue
                                }

                                friendData
                            }

                            // Convert enhanced list to JSON
                            val enhancedJson = mapper.writeValueAsString(enhancedFriendsList)

                            // Update Redis cache with enhanced data
                            try {
                                val cachedData = lettuce.getAccountCache(username)
                                if (cachedData != null) {
                                    val accountData = mapper.readValue(cachedData, Map::class.java) as MutableMap<String, Any?>
                                    accountData["friends"] = enhancedJson
                                    val updatedJson = mapper.writeValueAsString(accountData)
                                    withContext(Dispatchers.IO) {
                                        lettuce.syncCommands?.setex("account:$username", 3600, updatedJson)
                                    }
                                    logger.info(Component.text(
                                        "Updated Redis cache for $username with enhanced friends list after friendship removal",
                                        NamedTextColor.GREEN
                                    ))
                                } else {
                                    // If no cache exists, trigger full cache refresh
                                    lettuce.cachePlayerAccount(username)
                                    logger.info(Component.text(
                                        "Created new Redis cache for $username after friendship removal",
                                        NamedTextColor.GREEN
                                    ))
                                }
                            } catch (e: Exception) {
                                logger.warn(Component.text(
                                    "Failed to update Redis cache for $username: ${e.message}",
                                    NamedTextColor.YELLOW
                                ))
                            }

                        } catch (e: Exception) {
                            logger.warn(Component.text(
                                "Error processing Redis update for $username: ${e.message}",
                                NamedTextColor.YELLOW
                            ))
                        }
                    }

                    logger.info(Component.text(
                        "Completed Redis cache update for both users after friendship removal",
                        NamedTextColor.GREEN
                    ))
                } else {
                    logger.warn(Component.text(
                        "Redis not available for friendship removal cache update",
                        NamedTextColor.YELLOW
                    ))
                }
            } catch (e: Exception) {
                logger.warn(Component.text(
                    "Failed to update Redis cache for friendship removal: ${e.message}",
                    NamedTextColor.YELLOW
                ))
                // Continue execution - the database was updated successfully even if Redis failed
            }

            return true
        } catch (e: Exception) {
            logger.warn(Component.text(
                "Error removing friendship: ${e.message}",
                NamedTextColor.RED
            ))
            return false
        }
    }

    /**
     * Check if user exists
     */
    private suspend fun userExists(username: String): Boolean {
        val query = """
            SELECT 1 FROM accounts
            WHERE LOWER(username) = LOWER('${username.replace("'", "''")}');
        """.trimIndent()

        val result = jaSync.executeQuery(query)
        return result.rows.isNotEmpty()
    }

    /**
     * Parse JSON array into a list of friend objects with username and since fields
     */
    private fun parseNames(json: String): List<String> {
        if (json == "[]" || json.isBlank()) return emptyList()

        try {
            val list = mapper.readValue(json, List::class.java) as List<*>
            return list.mapNotNull {
                when (it) {
                    is String -> it
                    is Map<*, *> -> it["username"]?.toString()
                    else -> null
                }
            }
        } catch (e: Exception) {
            return emptyList()
        }
    }

    /**
     * Parse request senders from incoming_requests JSON
     */
    private fun parseRequestSenders(json: String): List<String> {
        if (json == "[]" || json.isBlank()) return emptyList()

        try {
            val list = mapper.readValue(json, List::class.java) as List<*>
            return list.mapNotNull {
                when (it) {
                    is Map<*, *> -> it["from"]?.toString()
                    else -> null
                }
            }
        } catch (e: Exception) {
            return emptyList()
        }
    }

    /**
     * Parse request recipients from outgoing_requests JSON
     */
    private fun parseRequestRecipients(json: String): List<String> {
        if (json == "[]" || json.isBlank()) return emptyList()

        try {
            val list = mapper.readValue(json, List::class.java) as List<*>
            return list.mapNotNull {
                when (it) {
                    is Map<*, *> -> it["to"]?.toString()
                    else -> null
                }
            }
        } catch (e: Exception) {
            return emptyList()
        }
    }

    /**
     * Result types for friend actions
     */
    enum class FriendActionResult {
        REQUEST_SENT,         // Friend request was sent
        ALREADY_FRIENDS,      // Users are already friends
        SELF_REQUEST,         // User tried to add themselves
        USER_NOT_FOUND,       // Target user doesn't exist
        REQUEST_ALREADY_SENT, // Request was already sent previously
        NOW_FRIENDS,          // Request accepted, users are now friends
        FRIEND_REMOVED,       // Friend was successfully removed
        REQUEST_CANCELLED,    // Outgoing request was cancelled
        NOT_FRIENDS,          // Users weren't friends to begin with
        REQUESTS_DISABLED     // Target has disabled friend requests
    }
}

