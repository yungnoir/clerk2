package twizzy.tech.clerk.util

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import net.kyori.adventure.text.Component
import net.kyori.adventure.text.format.NamedTextColor
import net.kyori.adventure.text.logger.slf4j.ComponentLogger
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import java.util.concurrent.ConcurrentHashMap

/**
 * Utility class to handle database operations with Redis-first approach and PostgreSQL fallback
 */
class CacheFallback(
    private val lettuce: Lettuce,
    private val jaSync: JaSync,
    private val logger: ComponentLogger
) {
    private val mapper = jacksonObjectMapper()

    // Add in-memory cache for bulk fetched data with short TTL
    private val bulkDataCache = ConcurrentHashMap<String, Pair<Map<String, Any?>, Long>>()
    private val BULK_CACHE_TTL_MS = 5_000L // 5 seconds

    /**
     * Get multiple data columns in one operation for optimization
     * @param username the account username to query
     * @param columns list of columns to retrieve (permissions, ranks, friends, etc.)
     * @return map of column name to value
     */
    suspend fun getBulkData(username: String, columns: List<String>): Map<String, Any?> {
        val cacheKey = "$username:${columns.joinToString(":")}"
        val currentTime = System.currentTimeMillis()

        // Check local memory cache first for very frequent requests
        bulkDataCache[cacheKey]?.let { (cachedData, expiry) ->
            if (currentTime < expiry) {
                return cachedData
            }
        }

        // Try Redis first
        val cachedJson = withContext(Dispatchers.IO) {
            try {
                lettuce.getAccountCache(username)
            } catch (e: Exception) {
                logger.warn(Component.text("Redis error getting bulk data: ${e.message}", NamedTextColor.YELLOW))
                null
            }
        }

        if (cachedJson != null) {
            try {
                val accountData = mapper.readValue(cachedJson, Map::class.java)
                val result = mutableMapOf<String, Any?>()
                for (column in columns) {
                    if (accountData.containsKey(column)) {
                        result[column] = accountData[column]
                    }
                }

                // If we have all requested columns, store in memory cache and return
                if (result.keys.containsAll(columns)) {
                    bulkDataCache[cacheKey] = result to (currentTime + BULK_CACHE_TTL_MS)
                    return result
                }
            } catch (e: Exception) {
                logger.warn(
                    Component.text(
                        "Error parsing Redis data for bulk fetch: ${e.message}",
                        NamedTextColor.YELLOW
                    )
                )
            }
        }

        // Fallback to PostgreSQL for all columns at once
        return getBulkDataFromPostgres(username, columns)
    }

    /**
     * Fallback method to get bulk data from PostgreSQL
     */
    private suspend fun getBulkDataFromPostgres(
        username: String,
        columns: List<String>
    ): Map<String, Any?> {
        try {
            val columnsStr = columns.joinToString(", ")
            val query =
                "SELECT $columnsStr FROM accounts WHERE LOWER(username) = LOWER('${username.replace("'", "''")}')"
            val result = jaSync.executeQuery(query)
            val row = result.rows.firstOrNull() ?: return emptyMap()

            val data = mutableMapOf<String, Any?>()
            for (column in columns) {
                data[column] = row.get(column)
            }

            // Cache the data in memory for future bulk fetches
            val cacheKey = "$username:${columns.joinToString(":")}"
            bulkDataCache[cacheKey] = data to (System.currentTimeMillis() + BULK_CACHE_TTL_MS)

            return data
        } catch (e: Exception) {
            logger.error(Component.text("PostgreSQL bulk lookup failed: ${e.message}", NamedTextColor.RED))
            return emptyMap()
        }
    }

    /**
     * Generic method to retrieve data from Redis first, falling back to PostgreSQL
     * @param username the account username to query
     * @param column the column to retrieve (permissions, ranks, friends, etc.)
     * @param transform optional function to transform the raw data
     * @return the retrieved data after transformation, or null if not found
     */
    suspend fun <T> getData(
        username: String,
        column: String,
        transform: (Any?) -> T
    ): T? {
        // Debug log - tracking call
        logger.info(Component.text("[DEBUG] Getting $column for $username", NamedTextColor.AQUA))

        // Try Redis first
        val cachedJson = withContext(Dispatchers.IO) {
            try {
                lettuce.getAccountCache(username)
            } catch (e: Exception) {
                logger.warn(Component.text("Redis error getting $column: ${e.message}", NamedTextColor.YELLOW))
                null
            }
        }

        if (cachedJson != null) {
            logger.info(Component.text("[DEBUG] Found cached data in Redis for $username", NamedTextColor.AQUA))
            try {
                val accountData = mapper.readValue(cachedJson, Map::class.java)
                if (accountData.containsKey(column)) {
                    val rawValue = accountData[column]
                    logger.info(
                        Component.text(
                            "[DEBUG] Redis raw value for $column: $rawValue (${rawValue?.javaClass?.name})",
                            NamedTextColor.AQUA
                        )
                    )
                    val transformed = transform(rawValue)
                    logger.info(Component.text("[DEBUG] Redis transformed value: $transformed", NamedTextColor.AQUA))
                    return transformed
                } else {
                    logger.info(Component.text("[DEBUG] Column $column not found in Redis data", NamedTextColor.YELLOW))
                }
            } catch (e: Exception) {
                logger.warn(Component.text("[DEBUG] Error parsing Redis data: ${e.message}", NamedTextColor.YELLOW))
            }
        } else {
            logger.info(
                Component.text(
                    "[DEBUG] No cached data in Redis for $username, falling back to PostgreSQL",
                    NamedTextColor.YELLOW
                )
            )
        }

        // Fallback to PostgreSQL
        val pgResult = getDataFromPostgres(username, column, transform)
        logger.info(Component.text("[DEBUG] PostgreSQL result: $pgResult", NamedTextColor.AQUA))
        return pgResult
    }

    /**
     * Fallback method to get data from PostgreSQL when Redis cache doesn't have it
     */
    private suspend fun <T> getDataFromPostgres(
        username: String,
        column: String,
        transform: (Any?) -> T
    ): T? {
        try {
            val query = "SELECT $column FROM accounts WHERE LOWER(username) = LOWER('${username.replace("'", "''")}')"
            logger.info(Component.text("[DEBUG] PostgreSQL query: $query", NamedTextColor.AQUA))

            val result = jaSync.executeQuery(query)
            logger.info(Component.text("[DEBUG] PostgreSQL result rows: ${result.rows.size}", NamedTextColor.AQUA))

            val row = result.rows.firstOrNull()
            if (row == null) {
                logger.info(Component.text("[DEBUG] No data found in PostgreSQL for $username", NamedTextColor.YELLOW))
                return null
            }

            val rawValue = row.get(column)
            logger.info(
                Component.text(
                    "[DEBUG] PostgreSQL raw value for $column: $rawValue (${rawValue?.javaClass?.name})",
                    NamedTextColor.AQUA
                )
            )

            // Special handling for timestamp conversion for last_seen
            if (column == "last_seen") {
                if (rawValue == null) {
                    logger.info(Component.text("[DEBUG] last_seen is null in database", NamedTextColor.YELLOW))
                    return null
                }

                // Convert various timestamp formats to epoch seconds (what Redis uses)
                val epochSeconds = convertToEpochSeconds(rawValue)
                logger.info(
                    Component.text(
                        "[DEBUG] Converted timestamp to epoch seconds: $epochSeconds",
                        NamedTextColor.AQUA
                    )
                )

                // Apply the transform function with our converted timestamp
                return transform(epochSeconds)
            }

            // Standard handling for non-timestamp data
            return transform(rawValue)
        } catch (e: Exception) {
            logger.error(Component.text("[DEBUG] PostgreSQL lookup failed: ${e.message}", NamedTextColor.RED))
            logger.error(Component.text("[DEBUG] Stack trace: ${e.stackTraceToString().take(300)}", NamedTextColor.RED))
            return null
        }
    }

    /**
     * Helper method to convert various timestamp formats to epoch seconds
     */
    private fun convertToEpochSeconds(rawValue: Any?): Long {
        return when (rawValue) {
            is Number -> {
                logger.info(Component.text("[DEBUG] Timestamp is already a number: $rawValue", NamedTextColor.AQUA))
                rawValue.toLong()
            }

            is String -> {
                logger.info(Component.text("[DEBUG] Timestamp is a string: '$rawValue'", NamedTextColor.AQUA))
                try {
                    if (rawValue.contains("-") && rawValue.contains(":")) {
                        // PostgreSQL timestamp format: "2025-05-20 01:22:21.000 -0700"
                        val parts = rawValue.split(" ")
                        val datePart = parts[0] // 2025-05-20
                        val timePart = if (parts.size > 1) parts[1] else "00:00:00" // 01:22:21.000

                        try {
                            // Try java.sql.Timestamp first (works well for standard format)
                            val timestamp = java.sql.Timestamp.valueOf("$datePart $timePart")
                            logger.info(
                                Component.text(
                                    "[DEBUG] Parsed timestamp with SQL Timestamp: $timestamp",
                                    NamedTextColor.GREEN
                                )
                            )
                            timestamp.time / 1000
                        } catch (e: Exception) {
                            logger.warn(
                                Component.text(
                                    "[DEBUG] SQL Timestamp parse failed: ${e.message}",
                                    NamedTextColor.YELLOW
                                )
                            )

                            // Try another approach with Instant
                            try {
                                val timestampStr = "${datePart}T${timePart}".replace(" ", "T")
                                // Add Z if there's no timezone
                                val instantStr = if (timestampStr.endsWith("Z") ||
                                    timestampStr.contains("+") ||
                                    (timestampStr.indexOf("-", datePart.length) > 0)
                                ) {
                                    timestampStr
                                } else {
                                    "${timestampStr}Z"
                                }

                                val instant = Instant.parse(instantStr)
                                logger.info(
                                    Component.text(
                                        "[DEBUG] Parsed timestamp with Instant: $instant",
                                        NamedTextColor.GREEN
                                    )
                                )
                                instant.epochSecond
                            } catch (e2: Exception) {
                                logger.error(
                                    Component.text(
                                        "[DEBUG] Instant parse failed: ${e2.message}",
                                        NamedTextColor.RED
                                    )
                                )
                                // Try to interpret as epoch seconds if it's just numbers
                                rawValue.toLongOrNull() ?: 0L
                            }
                        }
                    } else {
                        // It's probably already in epoch format
                        logger.info(
                            Component.text(
                                "[DEBUG] Assuming string is already epoch format",
                                NamedTextColor.AQUA
                            )
                        )
                        rawValue.toLongOrNull() ?: 0L
                    }
                } catch (e: Exception) {
                    logger.error(
                        Component.text(
                            "[DEBUG] All timestamp parsing methods failed: ${e.message}",
                            NamedTextColor.RED
                        )
                    )
                    0L
                }
            }

            is java.time.OffsetDateTime -> {
                logger.info(Component.text("[DEBUG] Timestamp is OffsetDateTime: $rawValue", NamedTextColor.AQUA))
                rawValue.toEpochSecond()
            }

            is java.sql.Timestamp -> {
                logger.info(Component.text("[DEBUG] Timestamp is SQL Timestamp: $rawValue", NamedTextColor.AQUA))
                rawValue.time / 1000
            }

            else -> {
                logger.error(
                    Component.text(
                        "[DEBUG] Unknown timestamp type: ${rawValue?.javaClass?.name}",
                        NamedTextColor.RED
                    )
                )
                0L
            }
        }
    }

    /**
     * Helper method to update data in Redis or PostgreSQL
     * @param username the account username
     * @param column the column to update
     * @param parser function to parse the raw value into the desired type
     * @param modifier function to modify the parsed value
     * @param serializer function to serialize the modified value back to string
     * @return true if update was successful
     */
    suspend fun <T> updateData(
        username: String,
        column: String,
        parser: (Any?) -> T,
        modifier: (T?) -> T?,
        serializer: (T?) -> String
    ): Boolean {
        try {
            // Try Redis first
            val cachedJson = withContext(Dispatchers.IO) {
                lettuce.getAccountCache(username)
            }

            if (cachedJson != null) {
                try {
                    val accountData = mapper.readValue(cachedJson, MutableMap::class.java) as MutableMap<String, Any?>

                    // Parse current value
                    val currentValue = if (accountData.containsKey(column)) {
                        parser(accountData[column])
                    } else {
                        null
                    }

                    // Modify the value
                    val newValue = modifier(currentValue)
                    if (newValue == null) {
                        return false
                    }

                    // Update the account data
                    accountData[column] = serializer(newValue)

                    // Save back to Redis with 1-hour TTL
                    val updatedJson = mapper.writeValueAsString(accountData)
                    withContext(Dispatchers.IO) {
                        lettuce.syncCommands?.setex("account:$username", 3600, updatedJson)
                    }

                    // Also update PostgreSQL for persistence
                    val query = """
                        UPDATE accounts
                        SET $column = '${serializer(newValue).replace("'", "''")}'::jsonb
                        WHERE LOWER(username) = LOWER('${username.replace("'", "''")}')
                    """.trimIndent()

                    jaSync.executeQuery(query)
                    return true
                } catch (e: Exception) {
                    logger.warn(
                        Component.text(
                            "[DEBUG] Error updating Redis data: ${e.message}",
                            NamedTextColor.YELLOW
                        )
                    )
                }
            }

            // If Redis update fails or no Redis data, update PostgreSQL directly
            val query = """
                UPDATE accounts
                SET $column = '${serializer(modifier(null)).replace("'", "''")}'::jsonb
                WHERE LOWER(username) = LOWER('${username.replace("'", "''")}')
            """.trimIndent()

            val result = jaSync.executeQuery(query)
            return result.rowsAffected > 0
        } catch (e: Exception) {
            logger.error(Component.text("[DEBUG] Error updating data: ${e.message}", NamedTextColor.RED))
            return false
        }
    }

    /**
     * Format time durations nicely for user display
     * @param timestamp The epoch seconds timestamp
     * @param useAgo If true, adds "ago" for past times
     */
    fun formatDuration(timestamp: Long, useAgo: Boolean = false): String {
        logger.info(Component.text("[DEBUG] Formatting duration for timestamp: $timestamp", NamedTextColor.AQUA))

        val now = Instant.now().epochSecond
        val secondsAgo = now - timestamp

        if (secondsAgo < 0) {
            logger.warn(Component.text("[DEBUG] Timestamp is in the future: $timestamp > $now", NamedTextColor.YELLOW))
            return "just now"
        }

        val minutes = secondsAgo / 60
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
     * Helper to parse JSON values into a List
     * @param value The JSON string or object to parse
     * @return A List containing the parsed elements
     */
    fun parseJsonToList(value: Any?): List<Any> {
        if (value == null) return emptyList()

        return try {
            when (value) {
                is List<*> -> value.filterNotNull()
                is String -> {
                    if (value.isBlank()) return emptyList()

                    if (value.startsWith("[")) {
                        mapper.readValue(value, List::class.java) as List<Any>
                    } else if (value.startsWith("{")) {
                        listOf(mapper.readValue(value, Map::class.java))
                    } else {
                        listOf(value)
                    }
                }
                is Map<*, *> -> listOf(value)
                else -> listOf(value)
            }
        } catch (e: Exception) {
            logger.warn(Component.text("Failed to parse JSON to list: $value - ${e.message}", NamedTextColor.YELLOW))
            emptyList()
        }
    }
}
