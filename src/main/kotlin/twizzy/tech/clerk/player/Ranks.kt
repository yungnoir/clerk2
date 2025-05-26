package twizzy.tech.clerk.player

import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.module.kotlin.readValue
import kotlinx.coroutines.runBlocking
import net.kyori.adventure.text.Component
import net.kyori.adventure.text.format.NamedTextColor
import twizzy.tech.clerk.Clerk
import twizzy.tech.clerk.util.JaSync
import twizzy.tech.clerk.util.JacksonFactory
import java.io.File
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Logger

class Ranks(private val clerk: Clerk) {

    @Target(AnnotationTarget.VALUE_PARAMETER)
    annotation class CachedRanks()

    private val jaSync = clerk.jaSync
    private val logger = Logger.getLogger("Ranks")
    private val ranksFile = File("ranks.yml")
    
    // In-memory cache for ranks and their permissions
    private val rankCache = ConcurrentHashMap<String, CachedRank>()

    // Data class for cached rank information - simplified to only include permissions and inheritance
    data class CachedRank(
        val name: String,
        val permissions: List<String>,
        val inheritance: List<String>
    )

    // Data class for a user rank with optional expiration
    data class UserRank(
        val rank: String,
        val expires: Long? = null
    ) {
        fun isExpired(): Boolean {
            return expires != null && expires < Instant.now().epochSecond
        }
        
        fun getExpirationTime(): OffsetDateTime? {
            return expires?.let { 
                OffsetDateTime.ofInstant(Instant.ofEpochSecond(it), ZoneOffset.UTC) 
            }
        }
    }
    
    // Initialize ranks system
    suspend fun initialize(): List<Clerk.DatabaseMessage> {
        val messages = mutableListOf<Clerk.DatabaseMessage>()
        
        messages.add(Clerk.DatabaseMessage("Initializing ranks system...", Clerk.MessageType.ATTEMPT))
        
        try {
            // First check if the ranks table exists
            val tableExists = checkRanksTableExists()
            if (!tableExists) {
                messages.add(Clerk.DatabaseMessage("Ranks table does not exist yet. It will be created during database initialization.", Clerk.MessageType.ATTEMPT))
                return messages
            }
            
            // Ensure default rank exists first
            ensureDefaultRank()

            // Update the YAML file from the database (not the other way around)
            updateYamlFromDatabase()
            
            // Refresh the in-memory rank cache
            refreshRankCache()

            messages.add(Clerk.DatabaseMessage("Ranks system initialized successfully", Clerk.MessageType.SUCCESS))
        } catch (e: Exception) {
            messages.add(Clerk.DatabaseMessage("Error initializing ranks system: ${e.message}", Clerk.MessageType.ERROR))
        }
        
        return messages
    }
    
    // Check if ranks table exists in the database
    private suspend fun checkRanksTableExists(): Boolean {
        val checkTableQuery = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'ranks'
            );
        """.trimIndent()
        
        val result = jaSync.executeQuery(checkTableQuery)
        return result.rows.firstOrNull()?.getBoolean(0) ?: false
    }
    
    // Update the YAML file with the current database content
    private suspend fun updateYamlFromDatabase() {
        try {
            // Load ranks from database
            val dbRanks = loadRanksFromDatabase()
            
            // Save to YAML, overwriting any existing data
            JacksonFactory.saveRanksConfig(dbRanks)
            
            logger.info("Updated ranks.yml file from database")
        } catch (e: Exception) {
            logger.warning("Error updating ranks.yml from database: ${e.message}")
            throw e
        }
    }
    
    // Load ranks from the database
    private suspend fun loadRanksFromDatabase(): JacksonFactory.RanksConfig {
        val config = JacksonFactory.RanksConfig()
        
        val result = jaSync.executeQuery("SELECT * FROM ranks ORDER BY weight DESC;")
        
        for (row in result.rows) {
            val name = row.getString("name") ?: continue
            val prefix = row.getString("prefix") ?: ""
            
            // Parse JSONB data
            val permissionsStr = row.getString("permissions") ?: "[]"
            val inheritanceStr = row.getString("inheritance") ?: "[]"
            val usersStr = row.getString("users") ?: "[]"
            
            // Parse JSON arrays
            val permissions = parseJsonArray(permissionsStr)
            val inheritance = parseJsonArray(inheritanceStr)
            val users = parseJsonArray(usersStr)
            
            val weight = row.getInt("weight") ?: 0
            val isDefault = name == "Default"
            
            config.ranks[name] = JacksonFactory.Rank(
                name = name,
                prefix = prefix,
                permissions = permissions,
                inheritance = inheritance,
                weight = weight,
                users = users,
                isDefault = isDefault
            )

            // Update in-memory cache
            rankCache[name] = CachedRank(
                name = name,
                permissions = permissions,
                inheritance = inheritance
            )
        }
        
        return config
    }
    
    // Parse JSON arrays from the database
    private fun parseJsonArray(jsonStr: String): List<String> {
        return try {
            if (jsonStr.isBlank() || jsonStr == "null") {
                emptyList()
            } else {
                val cleanJson = if (!jsonStr.startsWith("[")) "[$jsonStr]" else jsonStr
                JacksonFactory.mapper.readValue<List<String>>(cleanJson)
            }
        } catch (e: Exception) {
            logger.warning("Error parsing JSON array: $jsonStr - ${e.message}")
            emptyList()
        }
    }
    
    // Get user ranks from the database with expiration info
    suspend fun getUserRanks(username: String): List<UserRank> {
        try {
            val query = "SELECT ranks FROM accounts WHERE LOWER(username) = LOWER('${username.replace("'", "''")}');"
            val result = jaSync.executeQuery(query)

            if (result.rows.isEmpty()) {
                return emptyList()
            }

            val ranksJson = result.rows.first().getString("ranks") ?: return emptyList()
            if (ranksJson.isBlank() || ranksJson == "[]" || ranksJson == "null") {
                return emptyList()
            }

            val jsonNode = JacksonFactory.mapper.readTree(ranksJson)
            if (!jsonNode.isArray()) {
                return emptyList()
            }

            return (jsonNode as ArrayNode).mapNotNull { node ->
                if (node.isTextual) {
                    // Simple string rank - permanent rank
                    UserRank(node.asText())
                } else if (node.isObject() && node.has("rank")) {
                    // Object with rank and optional expiration
                    val rankName = node.get("rank").asText()
                    val expires = if (node.has("expires")) node.get("expires").asLong() else null
                    UserRank(rankName, expires)
                } else {
                    // Skip invalid entries
                    null
                }
            }
        } catch (e: Exception) {
            logger.warning("Error getting user ranks for $username: ${e.message}")
            return emptyList()
        }
    }
    
    // Save a rank to the database
    private suspend fun saveRankToDatabase(rank: JacksonFactory.Rank) {
        try {
            // Use proper parameter binding to avoid SQL injection and formatting issues
            // Properly escape prefix for database insertion
            val escapedPrefix = rank.prefix.replace("'", "''")
            val escapedName = rank.name.replace("'", "''")
            
            // For JSON fields, use PostgreSQL's jsonb_build_array function instead of string conversion
            // This prevents issues with special characters like hyphens
            val query = """
                INSERT INTO ranks (name, prefix, permissions, inheritance, weight, users)
                VALUES (
                    '$escapedName', 
                    '$escapedPrefix', 
                    jsonb_build_array(${rank.permissions.joinToString(",") { "'${it.replace("'", "''")}'" }}), 
                    jsonb_build_array(${rank.inheritance.joinToString(",") { "'${it.replace("'", "''")}'" }}), 
                    ${rank.weight},
                    jsonb_build_array(${rank.users.joinToString(",") { "'${it.replace("'", "''")}'" }})
                )
                ON CONFLICT (name) DO UPDATE
                SET 
                    prefix = '$escapedPrefix',
                    permissions = jsonb_build_array(${rank.permissions.joinToString(",") { "'${it.replace("'", "''")}'" }}),
                    inheritance = jsonb_build_array(${rank.inheritance.joinToString(",") { "'${it.replace("'", "''")}'" }}),
                    weight = ${rank.weight},
                    users = jsonb_build_array(${rank.users.joinToString(",") { "'${it.replace("'", "''")}'" }});
            """.trimIndent()
            
            jaSync.executeQuery(query)

            // Update in-memory cache
            rankCache[rank.name] = CachedRank(
                name = rank.name,
                permissions = rank.permissions,
                inheritance = rank.inheritance
            )
        } catch (e: Exception) {
            logger.warning("Error saving rank ${rank.name} to database: ${e.message}")
            throw e
        }
    }
    
    // Ensure default rank exists
    private suspend fun ensureDefaultRank() {
        // First check if Default rank exists in the database
        val checkQuery = "SELECT COUNT(*) FROM ranks WHERE name = 'Default';"
        val result = jaSync.executeQuery(checkQuery)
        // Fix: Use getLong instead of getInt for COUNT result
        val defaultExists = (result.rows.firstOrNull()?.getLong(0) ?: 0L) > 0
        
        if (!defaultExists) {
            // Create a new default rank with the specific name and prefix
            val defaultRank = JacksonFactory.Rank(
                name = "Default",
                prefix = "[Default]",
                permissions = listOf("clerk.default"),
                inheritance = emptyList(),
                weight = 0,
                isDefault = true
            )
            
            saveRankToDatabase(defaultRank)
            clerk.logger.info(Component.text("Created default rank 'Default' with prefix '[Default]'", NamedTextColor.GREEN))
            
            // Update the YAML file if it exists
            if (ranksFile.exists()) {
                val config = JacksonFactory.loadRanksConfig()
                config.ranks["Default"] = defaultRank
                JacksonFactory.saveRanksConfig(config)
            }
        }
    }
    
    // Save ranks from database to YAML
    suspend fun saveRanks(): Boolean {
        try {
            // Update YAML from database
            updateYamlFromDatabase()
            
            logger.info("Successfully saved ranks from database to YAML")
            return true
        } catch (e: Exception) {
            logger.warning("Error saving ranks from database to YAML: ${e.message}")
            return false
        }
    }
    
    // Load ranks from YAML to database
    suspend fun loadRanks(): Boolean {
        try {
            // Load ranks from YAML
            val yamlRanks = JacksonFactory.loadRanksConfig()
            
            if (yamlRanks.ranks.isEmpty()) {
                logger.warning("No ranks found in YAML file")
                return false
            }
            
            // Get current ranks from database to preserve users
            val dbRanks = loadRanksFromDatabase().ranks
            
            // Save each rank to the database, preserving user lists
            for (rank in yamlRanks.ranks.values) {
                // Keep users from database if the rank exists
                val users = dbRanks[rank.name]?.users ?: emptyList()
                
                saveRankToDatabase(rank.copy(
                    users = users,
                    isDefault = rank.name == "Default"
                ))
            }
            
            logger.info("Successfully loaded ${yamlRanks.ranks.size} ranks from YAML to database")

            clerk.lettuce.notifyRanksUpdate()
            return true
        } catch (e: Exception) {
            logger.warning("Error loading ranks from YAML to database: ${e.message}")
            return false
        }
    }
    
    // Get all ranks directly from the database
    fun getAllRanks(): Map<String, JacksonFactory.Rank> = runBlocking {
        loadRanksFromDatabase().ranks
    }
    
    // Get a specific rank directly from the database
    fun getRank(name: String): JacksonFactory.Rank? = runBlocking {
        val ranks = loadRanksFromDatabase().ranks
        ranks[name]
    }
    
    // Create or update a rank
    suspend fun setRank(
        name: String,
        prefix: String,
        permissions: List<String>,
        inheritance: List<String>,
        weight: Int?,
        users: List<String> = emptyList(),
        isDefault: Boolean = name == "Default"  // Only Default is default
    ): Boolean {
        try {
            // Get current users if the rank already exists in the database
            val existingRank = getRank(name)
            val currentUsers = existingRank?.users ?: users
            
            // Create rank with specified properties
            val rank = JacksonFactory.Rank(
                name = name, 
                prefix = prefix, 
                permissions = permissions, 
                inheritance = inheritance, 
                weight = weight, 
                users = currentUsers,
                isDefault = name == "Default"
            )
            
            // Save to database first
            saveRankToDatabase(rank)


            // Refresh the in-memory rank cache
            refreshRankCache()

            clerk.lettuce.notifyRanksUpdate()

            return true
        } catch (e: Exception) {
            logger.warning("Error setting rank $name: ${e.message}")
            return false
        }
    }
    
    // Delete a rank
    suspend fun deleteRank(name: String): Boolean {
        try {
            // Don't delete the default rank
            if (name.equals("Default", ignoreCase = true)) {
                return false
            }
            
            // First update inheritance in other ranks to remove this rank
            val updateInheritanceQuery = """
                UPDATE ranks
                SET inheritance = (
                    SELECT COALESCE(
                        jsonb_agg(value) FILTER (WHERE value != '"${name.replace("\"", "\\\"")}"'),
                        '[]'::jsonb
                    )
                    FROM jsonb_array_elements(inheritance) AS elem(value)
                )
                WHERE inheritance @> '["${name.replace("\"", "\\\"")}"]';
            """.trimIndent()
            jaSync.executeQuery(updateInheritanceQuery)

            // Delete from database next
            jaSync.executeQuery("DELETE FROM ranks WHERE name = '$name';")
            
            // Clean up accounts table - remove the rank from all users
            val cleanupQuery = """
                UPDATE accounts
                SET ranks = (
                    SELECT COALESCE(
                        jsonb_agg(elem) FILTER (WHERE elem IS NOT NULL),
                        '[]'::jsonb
                    )
                    FROM jsonb_array_elements(ranks) AS elem
                    WHERE (elem->>'rank' != '${name.replace("'", "''")}' AND elem != '"${name.replace("\"", "\\\"")}"')
                )
                WHERE (ranks @> '["${name.replace("\"", "\\\"")}"]' OR 
                      ranks @> '[{"rank":"${name.replace("\"", "\\\"")}"}]' OR
                      ranks::text LIKE '%"rank":"${name.replace("\"", "\\\"")}",%');
            """.trimIndent()
            jaSync.executeQuery(cleanupQuery)

            
            // Remove from in-memory cache
            rankCache.remove(name)

            // Notify via Redis PubSub
            clerk.lettuce.notifyRanksUpdate()

            logger.info("Deleted rank '$name', removed it from all accounts, and cleaned up inheritance references")
            return true
        } catch (e: Exception) {
            logger.warning("Error deleting rank $name: ${e.message}")
            return false
        }
    }

    // Grant a rank to a user with optional duration
    suspend fun grantRank(username: String, rankName: String, duration: String? = null): Boolean {
        try {
            val rank = getRank(rankName) ?: return false

            // Validate duration if provided
            val validDuration = duration?.let {
                val regex = """^(\d+)(s|m|h|d|w|mo|y)$""".toRegex()
                if (regex.matches(it)) it else null
            }

            // Parse duration if provided and valid
            val expirationEpoch = if (validDuration != null) {
                val expireTime = calculateExpirationTime(validDuration)
                expireTime?.toEpochSecond()
            } else if (duration != null) {
                // Invalid duration provided
                return false
            } else {
                null
            }
            
            // 1. Update the rank's users list in ranks table
            if (username !in rank.users) {
                val updatedUsers = rank.users + username
                val updatedRank = rank.copy(users = updatedUsers)
                saveRankToDatabase(updatedRank)
            }

            // 2. First check if the account exists and get current ranks
            val checkQuery = "SELECT ranks FROM accounts WHERE LOWER(username) = LOWER('${username.replace("'", "''")}');"
            val checkResult = jaSync.executeQuery(checkQuery)

            if (checkResult.rows.isEmpty()) {
                logger.warning("Account not found for username: $username")
                return false
            }

            // Make sure ranks column is initialized
            val initQuery = """
                UPDATE accounts 
                SET ranks = COALESCE(ranks, '[]'::jsonb)
                WHERE LOWER(username) = LOWER('${username.replace("'", "''")}') AND (ranks IS NULL OR ranks = 'null'::jsonb);
            """.trimIndent()
            jaSync.executeQuery(initQuery)

            // 3. Now update the ranks column with the new rank
            if (expirationEpoch != null) {
                // Add rank with expiration using PostgreSQL's jsonb functions
                val addRankQuery = """
                    UPDATE accounts 
                    SET ranks = (
                        SELECT COALESCE(
                            jsonb_agg(
                                CASE 
                                    WHEN (elem->>'rank' = '$rankName' OR elem = '"$rankName"')
                                    THEN NULL 
                                    ELSE elem 
                                END
                            ) FILTER (WHERE elem IS NOT NULL),
                            '[]'::jsonb
                        )
                        FROM jsonb_array_elements(COALESCE(ranks, '[]'::jsonb)) AS elem
                    ) || jsonb_build_array(
                        jsonb_build_object('rank', '$rankName', 'expires', $expirationEpoch)
                    )
                    WHERE LOWER(username) = LOWER('${username.replace("'", "''")}');
                """.trimIndent()
                
                jaSync.executeQuery(addRankQuery)
                
                val expireDate = Instant.ofEpochSecond(expirationEpoch)
                    .atOffset(ZoneOffset.UTC)
                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
                
                logger.info("Granted rank '$rankName' to user '$username' with expiration: $expireDate")
            } else {
                // Add permanent rank using consistent object format with rank property
                val addRankQuery = """
                    UPDATE accounts 
                    SET ranks = (
                        SELECT COALESCE(
                            jsonb_agg(
                                CASE 
                                    WHEN (elem->>'rank' = '$rankName' OR elem = '"$rankName"')
                                    THEN NULL 
                                    ELSE elem 
                                END
                            ) FILTER (WHERE elem IS NOT NULL),
                            '[]'::jsonb
                        )
                        FROM jsonb_array_elements(COALESCE(ranks, '[]'::jsonb)) AS elem
                    ) || jsonb_build_array(
                        jsonb_build_object('rank', '$rankName')
                    )
                    WHERE LOWER(username) = LOWER('${username.replace("'", "''")}');
                """.trimIndent()
                
                jaSync.executeQuery(addRankQuery)
                
                logger.info("Granted permanent rank '$rankName' to user '$username'")
            }

            // 4. Update Redis cache if the account is cached
            updateRankInRedisCache(username, rankName, expirationEpoch)

            return true
        } catch (e: Exception) {
            logger.warning("Error granting rank $rankName to user $username: ${e.message}")
            e.printStackTrace() // Log full stack trace for debugging
            return false
        }
    }

    // Remove a rank from a user
    suspend fun removeRank(username: String, rankName: String): Boolean {
        try {
            // 1. Update the rank's users list in ranks table
            val rank = getRank(rankName)
            if (rank != null && username in rank.users) {
                val updatedUsers = rank.users.filter { it != username }
                val updatedRank = rank.copy(users = updatedUsers)
                saveRankToDatabase(updatedRank)
            }

            // 2. Update the user's ranks column in accounts table using PostgreSQL's jsonb functions
            val removeRankQuery = """
                UPDATE accounts 
                SET ranks = (
                    SELECT jsonb_agg(elem)
                    FROM jsonb_array_elements(COALESCE(ranks, '[]'::jsonb)) elem
                    WHERE (elem->>'rank' != '$rankName' AND elem != '"$rankName"')
                )
                WHERE LOWER(username) = LOWER('${username.replace("'", "''")}');
            """.trimIndent()
            
            val result = jaSync.executeQuery(removeRankQuery)
            
            if (result.rowsAffected <= 0) {
                logger.warning("Failed to update ranks for user $username")
                return false
            }
            
            // 3. Update Redis cache if the account is cached
            removeRankFromRedisCache(username, rankName)

            logger.info("Removed rank '$rankName' from user '$username'")
            return true
        } catch (e: Exception) {
            logger.warning("Error removing rank $rankName from user $username: ${e.message}")
            return false
        }
    }


    /**
     * Updates a user's rank in Redis cache if the account is already cached
     */
    private suspend fun updateRankInRedisCache(username: String, rankName: String, expirationEpoch: Long?) {
        try {
            val lettuce = clerk.lettuce ?: return

            // Check if the account is cached in Redis
            val cachedJson = lettuce.getAccountCache(username) ?: return

            // Parse the cached account data - make sure to parse as a Map
            val mapper = com.fasterxml.jackson.module.kotlin.jacksonObjectMapper()
            val accountData = try {
                mapper.readValue(cachedJson, MutableMap::class.java) as MutableMap<String, Any?>
            } catch (e: Exception) {
                logger.warning("Error parsing account JSON from Redis: ${e.message}")
                return
            }

            // Get the ranks array or create a new one
            val updatedRanks = mapper.createArrayNode()

            if (accountData.containsKey("ranks")) {
                try {
                    // Get the current ranks array and validate it's properly formatted
                    val ranksValue = accountData["ranks"]
                    val ranksNode = when (ranksValue) {
                        is String -> mapper.readTree(ranksValue)
                        else -> mapper.valueToTree(ranksValue)
                    }

                    // Add all existing ranks except the one we're updating
                    if (ranksNode != null && ranksNode.isArray) {
                        ranksNode.forEach { node ->
                            if (node.isTextual && node.asText() != rankName) {
                                updatedRanks.add(node)
                            } else if (node.isObject && node.has("rank") && node.get("rank").asText() != rankName) {
                                updatedRanks.add(node)
                            }
                        }
                    }
                } catch (e: Exception) {
                    logger.warning("Error processing ranks from Redis: ${e.message}")
                    // Continue with empty ranks array
                }
            }

            // Add the new/updated rank using object format for consistency
            if (expirationEpoch != null) {
                val rankObj = mapper.createObjectNode()
                rankObj.put("rank", rankName)
                rankObj.put("expires", expirationEpoch)
                updatedRanks.add(rankObj)
            } else {
                val rankObj = mapper.createObjectNode()
                rankObj.put("rank", rankName)
                updatedRanks.add(rankObj)
            }

            // Update the ranks in the account data
            accountData["ranks"] = updatedRanks

            // Save the updated account data back to Redis
            val updatedJson = mapper.writeValueAsString(accountData)
            kotlinx.coroutines.withContext(kotlinx.coroutines.Dispatchers.IO) {
                lettuce.connection?.sync()?.setex("account:$username", 3600, updatedJson)
            }

            logger.info("Updated rank '$rankName' in Redis cache for user '$username'")

            clerk.lettuce.notifyAccountUpdate(username)
        } catch (e: Exception) {
            logger.warning("Error updating rank in Redis cache: ${e.message}")
            e.printStackTrace() // Add stack trace for better debugging
        }
    }

    /**
     * Removes a user's rank from Redis cache if the account is already cached
     */
    private suspend fun removeRankFromRedisCache(username: String, rankName: String) {
        try {
            val lettuce = clerk.lettuce ?: return

            // Check if the account is cached in Redis
            val cachedJson = lettuce.getAccountCache(username) ?: return

            // Parse the cached account data
            val mapper = com.fasterxml.jackson.module.kotlin.jacksonObjectMapper()
            val accountData = try {
                mapper.readValue(cachedJson, MutableMap::class.java) as MutableMap<String, Any?>
            } catch (e: Exception) {
                logger.warning("Error parsing account JSON from Redis: ${e.message}")
                return
            }

            // Check if the account has ranks
            if (!accountData.containsKey("ranks")) return

            // Create a new ranks array
            val updatedRanks = mapper.createArrayNode()

            try {
                // Get the current ranks array and validate it's properly formatted
                val ranksValue = accountData["ranks"]
                val ranksNode = when (ranksValue) {
                    is String -> mapper.readTree(ranksValue)
                    else -> mapper.valueToTree(ranksValue)
                }

                // Add all existing ranks except the one being removed
                if (ranksNode != null && ranksNode.isArray) {
                    ranksNode.forEach { node ->
                        if (node.isTextual && node.asText() != rankName) {
                            updatedRanks.add(node)
                        } else if (node.isObject && node.has("rank") && node.get("rank").asText() != rankName) {
                            updatedRanks.add(node)
                        }
                    }
                }
            } catch (e: Exception) {
                logger.warning("Error processing ranks from Redis: ${e.message}")
                // Continue with empty ranks array
            }

            // Update the ranks in the account data
            accountData["ranks"] = updatedRanks

            // Save the updated account data back to Redis
            val updatedJson = mapper.writeValueAsString(accountData)
            kotlinx.coroutines.withContext(kotlinx.coroutines.Dispatchers.IO) {
                lettuce.connection?.sync()?.setex("account:$username", 3600, updatedJson)
            }

            logger.info("Removed rank '$rankName' from Redis cache for user '$username'")

            clerk.lettuce.notifyAccountUpdate(username)
        } catch (e: Exception) {
            logger.warning("Error removing rank from Redis cache: ${e.message}")
            e.printStackTrace() // Add stack trace for better debugging
            // Non-critical error - the Redis cache will be updated when the account is cached again
        }
    }

    // Check if a user has a specific rank (accounting for expiration)
    suspend fun hasRank(username: String, rankName: String): Boolean {
        val userRanks = getUserRanks(username)
        return userRanks.any { it.rank == rankName && !it.isExpired() }
    }

    // Parse duration strings like "1s", "1m", "1h", "1d", "1w", "1mo", "1y" into a time duration
    private fun parseDuration(durationStr: String): java.time.Duration? {
        try {
            val regex = """(\d+)(s|m|h|d|w|mo|y)""".toRegex()
            val matchResult = regex.matchEntire(durationStr) ?: return null

            val amount = matchResult.groupValues[1].toLong()
            val unit = matchResult.groupValues[2]

            return when (unit) {
                "s" -> java.time.Duration.ofSeconds(amount)
                "m" -> java.time.Duration.ofMinutes(amount)
                "h" -> java.time.Duration.ofHours(amount)
                "d" -> java.time.Duration.ofDays(amount)
                "w" -> java.time.Duration.ofDays(amount * 7)
                // For months and years, return null (handled in calculateExpirationTime)
                "mo", "y" -> null
                else -> null
            }
        } catch (e: Exception) {
            logger.warning("Failed to parse duration: $durationStr - ${e.message}")
            return null
        }
    }

    // Calculate expiration time based on duration string, supporting months and years
    private fun calculateExpirationTime(durationStr: String): OffsetDateTime? {
        val regex = """(\d+)(s|m|h|d|w|mo|y)""".toRegex()
        val matchResult = regex.matchEntire(durationStr) ?: return null
        val amount = matchResult.groupValues[1].toLong()
        val unit = matchResult.groupValues[2]
        val now = OffsetDateTime.now()
        return when (unit) {
            "s" -> now.plusSeconds(amount)
            "m" -> now.plusMinutes(amount)
            "h" -> now.plusHours(amount)
            "d" -> now.plusDays(amount)
            "w" -> now.plusWeeks(amount)
            "mo" -> now.plusMonths(amount)
            "y" -> now.plusYears(amount)
            else -> null
        }
    }

    // Format time until expiration as a readable string
    fun formatTimeUntilExpiration(expirationTime: OffsetDateTime): String {
        val now = OffsetDateTime.now()
        val duration = java.time.Duration.between(now, expirationTime)

        if (duration.isNegative) {
            return "Expired"
        }

        val days = duration.toDays()
        val hours = duration.toHoursPart()
        val minutes = duration.toMinutesPart()

        return when {
            days > 0 -> "${days}d"
            hours > 0 -> "${hours}h"
            else -> "${minutes}m"
        }
    }

    /**
     * Refreshes the in-memory rank cache from the database
     * This should be called during initialization and after any synchronization
     */
    suspend fun refreshRankCache() {
        try {
            // Clear existing cache
            rankCache.clear()

            // Query all ranks from database
            val result = jaSync.executeQuery("SELECT * FROM ranks ORDER BY weight DESC;")

            // Process each rank
            for (row in result.rows) {
                val name = row.getString("name") ?: continue
                val prefix = row.getString("prefix") ?: ""

                // Parse JSONB data
                val permissionsStr = row.getString("permissions") ?: "[]"
                val inheritanceStr = row.getString("inheritance") ?: "[]"

                // Parse JSON arrays
                val permissions = parseJsonArray(permissionsStr)
                val inheritance = parseJsonArray(inheritanceStr)

                val weight = row.getInt("weight") ?: 0
                val isDefault = name == "Default"

                // Update cache
                rankCache[name] = CachedRank(
                    name = name,
                    permissions = permissions,
                    inheritance = inheritance
                )
            }

            logger.info("Rank cache refreshed with ${rankCache.size} ranks")
        } catch (e: Exception) {
            logger.warning("Error refreshing rank cache: ${e.message}")
        }
    }

    /**
     * Gets a rank from the in-memory cache
     * @param name The name of the rank to get
     * @return The cached rank, or null if not found
     */
    fun getCachedRank(name: String): CachedRank? {
        return rankCache[name]
    }

    /**
     * Gets all ranks from the in-memory cache
     * @return A map of rank names to cached ranks
     */
    fun getAllCachedRanks(): Map<String, CachedRank> {
        return rankCache.toMap()
    }

    /**
     * Gets all permissions for a rank, including inherited permissions
     * @param rankName The name of the rank to get permissions for
     * @return A list of all permissions for the rank, including inherited permissions
     */
    fun getAllPermissionsForRank(rankName: String): List<String> {
        val rank = getCachedRank(rankName) ?: return emptyList()
        val allPermissions = mutableSetOf<String>()

        // Add direct permissions
        allPermissions.addAll(rank.permissions)

        // Add inherited permissions (recursive)
        addInheritedPermissions(rank.inheritance, allPermissions, mutableSetOf())

        return allPermissions.toList()
    }

    /**
     * Recursively adds inherited permissions to the permissions set
     * @param inheritedRanks The ranks to get permissions from
     * @param allPermissions The set to add permissions to
     * @param processedRanks Ranks that have already been processed (to prevent infinite recursion)
     */
    private fun addInheritedPermissions(
        inheritedRanks: List<String>,
        allPermissions: MutableSet<String>,
        processedRanks: MutableSet<String>
    ) {
        for (inheritedRankName in inheritedRanks) {
            // Skip if already processed to prevent circular inheritance
            if (inheritedRankName in processedRanks) continue

            // Mark as processed
            processedRanks.add(inheritedRankName)

            // Get the inherited rank
            val inheritedRank = getCachedRank(inheritedRankName) ?: continue

            // Add permissions from this rank
            allPermissions.addAll(inheritedRank.permissions)

            // Recursively add permissions from ranks this rank inherits from
            addInheritedPermissions(inheritedRank.inheritance, allPermissions, processedRanks)
        }
    }
}

