package twizzy.tech.clerk.player

import com.github.jasync.sql.db.RowData
import com.velocitypowered.api.proxy.Player
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import net.kyori.adventure.text.Component
import net.kyori.adventure.text.format.NamedTextColor
import net.kyori.adventure.text.logger.slf4j.ComponentLogger
import org.geysermc.floodgate.api.FloodgateApi
import org.json.JSONObject
import twizzy.tech.clerk.Clerk
import twizzy.tech.clerk.util.CacheFallback
import twizzy.tech.clerk.util.JaSync
import twizzy.tech.clerk.util.Lettuce
import twizzy.tech.clerk.util.PostSchema
import java.net.URL
import java.time.Instant
import java.time.OffsetDateTime

class Account(private val clerk: Clerk) {

    private val jaSync = clerk.jaSync
    private val logger = clerk.logger

    @Target(AnnotationTarget.VALUE_PARAMETER)
    annotation class CachedAccounts()

    suspend fun registerAccount(player: Player, username: String, password: String): Map<String, Any?> {
        val details = mutableMapOf<String, Any?>()
        details["username"] = username
        details["password"] = "crypt('$password', gen_salt('bf'))"

        val isBedrock = FloodgateApi.getInstance().isFloodgatePlayer(player.uniqueId)
        val id = if (isBedrock) {
            FloodgateApi.getInstance().getPlayer(player.uniqueId)?.xuid ?: player.uniqueId.toString()
        } else {
            player.uniqueId.toString()
        }
        details["platform"] = if (isBedrock) "Bedrock" else "Java"
        details["id"] = id // Use "id" or your schema's column name

        val ip = player.remoteAddress.address.hostAddress
        val location = fetchLocationData(ip)
        details["ip_address"] = "[\"$ip\"]"
        details["uuids"] = "[\"$id\"]"
        details["country"] = location?.country?.takeIf { it.isNotBlank() } ?: "Unknown"
        details["region"] = location?.regionName?.takeIf { it.isNotBlank() } ?: "Unknown"
        details["registered_date"] = Instant.now().toString()
        
        // Explicitly initialize ranks as an empty JSON array
        details["ranks"] = "[]"

        val schemaColumns = PostSchema.AccountsTableSchema.columns
        schemaColumns.forEach { column ->
            if (!details.containsKey(column.name)) {
                details[column.name] = when {
                    column.type.contains("jsonb", ignoreCase = true) -> "[]"
                    column.type.contains("boolean", ignoreCase = true) -> false
                    column.type.contains("int", ignoreCase = true) -> 0
                    column.type.contains("timestamp", ignoreCase = true) -> null
                    column.type.startsWith("varchar", ignoreCase = true) ||
                            column.type.startsWith("text", ignoreCase = true) -> ""
                    else -> null
                }
            }
        }

        return details
    }

    suspend fun loginAccount(player: Player, username: String, password: String): Boolean {
        val ip = player.remoteAddress.address.hostAddress
        val isBedrock = FloodgateApi.getInstance().isFloodgatePlayer(player.uniqueId)
        val uuidOrXuid = if (isBedrock) {
            FloodgateApi.getInstance().getPlayer(player.uniqueId)?.xuid ?: player.uniqueId.toString()
        } else {
            player.uniqueId.toString()
        }
        val platform = if (isBedrock) "Bedrock" else "Java"
        val now = Instant.now().toString()

        val checkLockAndCredentialsQuery = """
            SELECT 
                username, 
                locked, 
                lock_until, 
                lock_reason, 
                failed_attempts,
                (password = crypt('${password.replace("'", "''")}', password)) as password_match
            FROM accounts 
            WHERE LOWER(username) = LOWER('${username.replace("'", "''")}');
        """.trimIndent()
        
        val result = jaSync.executeQuery(checkLockAndCredentialsQuery)
        
        if (result.rows.isEmpty()) {
            player.sendMessage(Component.text("Invalid username or password.", NamedTextColor.RED))
            return false
        }
        
        val row = result.rows[0]
        val locked = row.getBoolean("locked") ?: false
        val lockUntil = row.getTimestampOrNull("lock_until")
        val lockReason = row.getStringOrNull("lock_reason") ?: ""
        val failedAttempts = row.getInt("failed_attempts") ?: 0
        val passwordMatch = row.getBoolean("password_match") ?: false
        
        val currentTime = OffsetDateTime.now()
        
        if (locked && (lockUntil == null || currentTime.isBefore(lockUntil))) {
            val lockMessage = if (lockUntil != null) {
                "Your account is locked until ${lockUntil.format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))}. Reason: $lockReason"
            } else {
                "Your account is permanently locked. Reason: $lockReason. Please contact an administrator."
            }
            player.sendMessage(Component.text(lockMessage, NamedTextColor.RED))
            return false
        } else if (locked && currentTime.isAfter(lockUntil)) {
            val unlockQuery = """
                UPDATE accounts 
                SET locked = FALSE, lock_until = NULL, lock_reason = '' 
                WHERE LOWER(username) = LOWER('${username.replace("'", "''")}');
            """.trimIndent()
            jaSync.executeQuery(unlockQuery)
        }

        if (passwordMatch) {
            val accountQuery = """
                SELECT country, region FROM accounts
                WHERE LOWER(username) = LOWER('${username.replace("'", "''")}');
            """.trimIndent()
            val accountResult = jaSync.executeQuery(accountQuery)
            
            if (accountResult.rows.isNotEmpty()) {
                val storedCountry = accountResult.rows[0].getString("country") ?: "Unknown"
                val storedRegion = accountResult.rows[0].getString("region") ?: "Unknown"
                
                val ipInfo = fetchLocationData(ip)
                val currentCountry = ipInfo?.country ?: "Unknown"
                val currentRegion = ipInfo?.regionName ?: "Unknown"
                val isMobile = ipInfo?.mobile ?: false
                val isProxy = ipInfo?.proxy ?: false
                val isHosting = ipInfo?.hosting ?: false
                
                if (storedCountry != "Unknown" && currentCountry != "Unknown" && storedCountry != currentCountry) {
                    val lockQuery = """
                        UPDATE accounts 
                        SET locked = TRUE,
                            lock_reason = 'Suspicious login: Different country (${storedCountry} vs ${currentCountry})'
                        WHERE LOWER(username) = LOWER('${username.replace("'", "''")}');
                    """.trimIndent()
                    jaSync.executeQuery(lockQuery)
                    
                    player.sendMessage(Component.text(
                        "Security alert: Login from a different country detected. Your account has been locked.",
                        NamedTextColor.RED
                    ))
                    return false
                }
                
                if (storedCountry == currentCountry && storedRegion != "Unknown" && currentRegion != "Unknown" 
                    && storedRegion != currentRegion) {
                    
                    if ((isProxy || isHosting) && !isMobile) {
                        val lockQuery = """
                            UPDATE accounts 
                            SET locked = TRUE,
                                lock_reason = 'Suspicious login: Different region using proxy/hosting'
                            WHERE LOWER(username) = LOWER('${username.replace("'", "''")}');
                        """.trimIndent()
                        jaSync.executeQuery(lockQuery)
                        
                        player.sendMessage(Component.text(
                            "Security alert: Login from a different region via proxy detected. Your account has been locked.",
                            NamedTextColor.RED
                        ))
                        return false
                    }
                }

                // Optimize login storage - keep only last 10 entries, removing entries older than 7 days first
                val purgeOldLoginsQuery = """
                    UPDATE accounts
                    SET logins = (
                        SELECT COALESCE(
                            CASE 
                                WHEN jsonb_array_length(COALESCE(logins, '[]'::jsonb)) > 10
                                THEN (
                                    SELECT jsonb_agg(login)
                                    FROM (
                                        SELECT login
                                        FROM jsonb_array_elements(logins) AS login
                                        WHERE 
                                            (login->>'date') IS NOT NULL AND
                                            (
                                                CASE 
                                                    WHEN (login->>'date') ~ '\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}'
                                                    THEN to_timestamp((login->>'date'), 'YYYY-MM-DD"T"HH24:MI:SS')::timestamp > (NOW() - INTERVAL '7 days')
                                                    ELSE true
                                                END
                                            )
                                        ORDER BY (
                                            CASE
                                                WHEN (login->>'date') ~ '\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}'
                                                THEN to_timestamp((login->>'date'), 'YYYY-MM-DD"T"HH24:MI:SS')::timestamp
                                                ELSE NOW()
                                            END
                                        ) DESC
                                        LIMIT 10
                                    ) subquery
                                )
                                ELSE logins
                            END,
                            '[]'::jsonb
                        )
                    )
                    WHERE LOWER(username) = LOWER('${username.replace("'", "''")}');
                """.trimIndent()

                // Include location data in the login update
                val updates = listOf(
                    """
                    UPDATE accounts 
                    SET failed_attempts = 0, 
                        locked = FALSE, 
                        lock_until = NULL, 
                        lock_reason = '' 
                    WHERE LOWER(username) = LOWER('${username.replace("'", "''")}');
                    """.trimIndent(),

                    purgeOldLoginsQuery,

                    """
                    UPDATE accounts
                    SET logins = COALESCE(logins, '[]'::jsonb) || jsonb_build_object(
                        'id', '$uuidOrXuid',
                        'platform', '$platform',
                        'ip_address', '$ip',
                        'date', '$now',
                        'country', '${currentCountry.replace("'", "''")}',
                        'region', '${currentRegion.replace("'", "''")}'
                    ),
                    logged_out = FALSE
                    WHERE LOWER(username) = LOWER('${username.replace("'", "''")}');
                    """.trimIndent(),

                    """
                    UPDATE accounts
                    SET uuids = (
                        CASE
                            WHEN NOT (uuids @> '["$uuidOrXuid"]'::jsonb)
                            THEN uuids || '["$uuidOrXuid"]'::jsonb
                            ELSE uuids
                        END
                    )
                    WHERE LOWER(username) = LOWER('${username.replace("'", "''")}');
                    """.trimIndent()
                )

                jaSync.executeBatch(updates)
            } else {
                // Fallback case if account row was not found (shouldn't happen)
                val ipInfo = fetchLocationData(ip)
                val currentCountry = ipInfo?.country ?: "Unknown"
                val currentRegion = ipInfo?.regionName ?: "Unknown"

                val purgeOldLoginsQuery = """
                    UPDATE accounts
                    SET logins = (
                        SELECT COALESCE(
                            CASE 
                                WHEN jsonb_array_length(COALESCE(logins, '[]'::jsonb)) > 10
                                THEN (
                                    SELECT jsonb_agg(login)
                                    FROM (
                                        SELECT login
                                        FROM jsonb_array_elements(logins) AS login
                                        WHERE 
                                            (login->>'date') IS NOT NULL AND
                                            (
                                                CASE 
                                                    WHEN (login->>'date') ~ '\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}'
                                                    THEN to_timestamp((login->>'date'), 'YYYY-MM-DD"T"HH24:MI:SS')::timestamp > (NOW() - INTERVAL '7 days')
                                                    ELSE true
                                                END
                                            )
                                        ORDER BY (
                                            CASE
                                                WHEN (login->>'date') ~ '\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}'
                                                THEN to_timestamp((login->>'date'), 'YYYY-MM-DD"T"HH24:MI:SS')::timestamp
                                                ELSE NOW()
                                            END
                                        ) DESC
                                        LIMIT 10
                                    ) subquery
                                )
                                ELSE logins
                            END,
                            '[]'::jsonb
                        )
                    )
                    WHERE LOWER(username) = LOWER('${username.replace("'", "''")}');
                """.trimIndent()

                val updates = listOf(
                    """
                    UPDATE accounts 
                    SET failed_attempts = 0, 
                        locked = FALSE, 
                        lock_until = NULL, 
                        lock_reason = '' 
                    WHERE LOWER(username) = LOWER('${username.replace("'", "''")}');
                    """.trimIndent(),

                    purgeOldLoginsQuery,

                    """
                    UPDATE accounts
                    SET logins = COALESCE(logins, '[]'::jsonb) || jsonb_build_object(
                        'id', '$uuidOrXuid',
                        'platform', '$platform',
                        'ip_address', '$ip',
                        'date', '$now',
                        'country', '${currentCountry.replace("'", "''")}',
                        'region', '${currentRegion.replace("'", "''")}'
                    ),
                    logged_out = FALSE
                    WHERE LOWER(username) = LOWER('${username.replace("'", "''")}');
                    """.trimIndent(),
                    """
                    UPDATE accounts
                    SET uuids = (
                        CASE
                            WHEN NOT (uuids @> '["$uuidOrXuid"]'::jsonb)
                            THEN uuids || '["$uuidOrXuid"]'::jsonb
                            ELSE uuids
                        END
                    )
                    WHERE LOWER(username) = LOWER('${username.replace("'", "''")}');
                    """.trimIndent()
                )

                jaSync.executeBatch(updates)
            }

            return true
        } else {
            val usernameExistsQuery = """
                SELECT 1 FROM accounts
                WHERE LOWER(username) = LOWER('${username.replace("'", "''")}');
            """.trimIndent()
            val usernameExists = jaSync.executeQuery(usernameExistsQuery).rows.isNotEmpty()
            
            if (usernameExists) {
                val incrementQuery = """
                    UPDATE accounts 
                    SET failed_attempts = failed_attempts + 1 
                    WHERE LOWER(username) = LOWER('${username.replace("'", "''")}');
                """.trimIndent()
                jaSync.executeQuery(incrementQuery)
                
                val attemptsQuery = """
                    SELECT failed_attempts FROM accounts 
                    WHERE LOWER(username) = LOWER('${username.replace("'", "''")}');
                """.trimIndent()
                val attemptsResult = jaSync.executeQuery(attemptsQuery)
                
                if (attemptsResult.rows.isNotEmpty()) {
                    val attempts = attemptsResult.rows[0].getInt("failed_attempts") ?: 0
                    
                    if (attempts < 5) {
                        val remainingAttempts = 5 - attempts
                        player.sendMessage(Component.text(
                            "Invalid password. You have $remainingAttempts more attempts before temporary lockout.",
                            NamedTextColor.RED
                        ))
                    }
                    
                    when (attempts) {
                        5 -> {
                            val lockUntil = OffsetDateTime.now().plusMinutes(5)
                            val lockQuery = """
                                UPDATE accounts 
                                SET locked = TRUE, 
                                    lock_until = '${lockUntil}', 
                                    lock_reason = 'Too many failed login attempts (5). Account locked for 5 minutes.' 
                                WHERE LOWER(username) = LOWER('${username.replace("'", "''")}');
                            """.trimIndent()
                            jaSync.executeQuery(lockQuery)
                            player.sendMessage(Component.text("Account locked for 5 minutes due to too many failed login attempts.", NamedTextColor.RED))
                            player.currentServer.ifPresent { server ->
                                logger.info(
                                    Component.text("Account $username locked for 5 minutes due to failed attempts.", NamedTextColor.RED)
                                )
                            }
                        }
                        10 -> {
                            val lockUntil = OffsetDateTime.now().plusMinutes(30)
                            val lockQuery = """
                                UPDATE accounts 
                                SET locked = TRUE, 
                                    lock_until = '${lockUntil}', 
                                    lock_reason = 'Too many failed login attempts (10). Account locked for 30 minutes.' 
                                WHERE LOWER(username) = LOWER('${username.replace("'", "''")}');
                            """.trimIndent()
                            jaSync.executeQuery(lockQuery)
                            player.sendMessage(Component.text("Account locked for 30 minutes due to too many failed login attempts.", NamedTextColor.RED))
                            player.currentServer.ifPresent { server ->
                                logger.info(
                                    Component.text("Account $username locked for 30 minutes due to failed attempts.", NamedTextColor.RED)
                                )
                            }
                        }
                        15 -> {
                            val lockUntil = OffsetDateTime.now().plusHours(1)
                            val lockQuery = """
                                UPDATE accounts 
                                SET locked = TRUE, 
                                    lock_until = '${lockUntil}', 
                                    lock_reason = 'Too many failed login attempts (15). Account locked for 1 hour.' 
                                WHERE LOWER(username) = LOWER('${username.replace("'", "''")}');
                            """.trimIndent()
                            jaSync.executeQuery(lockQuery)
                            player.sendMessage(Component.text("Account locked for 1 hour due to too many failed login attempts.", NamedTextColor.RED))
                            player.currentServer.ifPresent { server ->
                                logger.info(
                                    Component.text("Account $username locked for 1 hour due to failed attempts.", NamedTextColor.RED)
                                )
                            }
                        }
                        20 -> {
                            val lockQuery = """
                                UPDATE accounts 
                                SET locked = TRUE, 
                                    lock_until = NULL, 
                                    lock_reason = 'Too many failed login attempts (20+). Account permanently locked.' 
                                WHERE LOWER(username) = LOWER('${username.replace("'", "''")}');
                            """.trimIndent()
                            jaSync.executeQuery(lockQuery)
                            player.sendMessage(Component.text("Account permanently locked due to excessive failed login attempts. Please contact an administrator.", NamedTextColor.RED))
                            player.currentServer.ifPresent { server ->
                                logger.info(
                                    Component.text("Account $username permanently locked due to failed attempts.", NamedTextColor.RED)
                                )
                            }
                        }
                    }
                }
            } else {
                player.sendMessage(Component.text("Invalid username or password.", NamedTextColor.RED))
            }
        }

        return false
    }

    private suspend fun fetchLocationData(ip: String): IpInfo? = withContext(Dispatchers.IO) {
        try {
            val url = URL("http://ip-api.com/json/$ip?fields=country,regionName,mobile,proxy,hosting")
            val json = url.readText()
            val obj = JSONObject(json)
            IpInfo(
                country = obj.optString("country"),
                regionName = obj.optString("regionName"),
                mobile = obj.optBoolean("mobile"),
                proxy = obj.optBoolean("proxy"),
                hosting = obj.optBoolean("hosting")
            )
        } catch (_: Exception) {
            null
        }
    }

    data class IpInfo(
        val country: String,
        val regionName: String,
        val mobile: Boolean,
        val proxy: Boolean,
        val hosting: Boolean
    )

    private fun RowData.getStringOrNull(columnName: String): String? {
        return try {
            getString(columnName)
        } catch (_: Exception) {
            null
        }
    }
    
    private fun RowData.getTimestampOrNull(columnName: String): OffsetDateTime? {
        return try {
            getAs<OffsetDateTime>(columnName)
        } catch (_: Exception) {
            null
        }
    }

    suspend fun getLastLoginUsername(id: String, ip: String): String? {
        val query = """
        SELECT username
        FROM accounts, jsonb_array_elements(logins) AS login
        WHERE '$id' = ANY (SELECT jsonb_array_elements_text(uuids))
        AND '$ip' = ANY (SELECT jsonb_array_elements_text(ip_address))
        AND logged_out = FALSE
        ORDER BY login->>'date' DESC
        LIMIT 1;
        """.trimIndent()
        val result = jaSync.executeQuery(query)
        return if (result.rows.isNotEmpty()) {
            result.rows.first().getString("username")
        } else {
            null
        }
    }

    suspend fun setLoggedOut(username: String, value: Boolean) {
        val query = """
            UPDATE accounts
            SET logged_out = ${if (value) "TRUE" else "FALSE"}
            WHERE LOWER(username) = LOWER('$username');
        """.trimIndent()
        jaSync.executeQuery(query)
    }

    data class AccountInfo(
        val username: String,
        val registeredDate: String,
        val javaUuids: List<String>,
        val xboxIds: List<String>,
        val country: String,
        val regionName: String,
        val ipAddresses: List<String> = emptyList()
    )
    
    suspend fun findAccountsByIDorIP(id: String, ip: String): List<AccountInfo> {
        val query = """
            SELECT username, registered_date, uuids, ip_address, country, region
            FROM accounts
            WHERE 
                ('$id' = ANY (SELECT jsonb_array_elements_text(uuids)))
                OR
                ('$ip' = ANY (SELECT jsonb_array_elements_text(ip_address)))
        """.trimIndent()
        val result = jaSync.executeQuery(query)
        return result.rows.map { row ->
            val usernameVal = row.getString("username") ?: ""
            val registeredDateObj = row["registered_date"]
            val registeredDate = when (registeredDateObj) {
                is String -> registeredDateObj
                is OffsetDateTime -> registeredDateObj.toString()
                else -> registeredDateObj?.toString() ?: ""
            }
            val uuidsJson = row.getString("uuids") ?: "[]"
            val uuids = parseJsonArray(uuidsJson)
            val javaUuids = uuids.filter { it.length == 36 }
            val xboxIds = uuids.filter { it.length != 36 }
            val ipJson = row.getString("ip_address") ?: "[]"
            val ipAddresses = parseJsonArray(ipJson)
            val country = row.getString("country") ?: ""
            val regionName = row.getString("region") ?: ""
            AccountInfo(
                username = usernameVal,
                registeredDate = registeredDate,
                javaUuids = javaUuids,
                xboxIds = xboxIds,
                country = country,
                regionName = regionName,
                ipAddresses = ipAddresses
            )
        }
    }

    suspend fun findAccountsAssociated(name: String): List<AccountInfo> {
        val visitedUsernames = mutableSetOf<String>()
        val visitedUuids = mutableSetOf<String>()
        val visitedIps = mutableSetOf<String>()
        val accounts = mutableListOf<AccountInfo>()

        suspend fun searchAndCollect(usernames: Set<String>) {
            if (usernames.isEmpty()) return
            val inClause = usernames.joinToString(",") { "'${it.replace("'", "''")}'" }
            val query = """
                SELECT username, registered_date, uuids, ip_address, country, region
                FROM accounts
                WHERE LOWER(username) IN (${usernames.joinToString(",") { "LOWER('${it.replace("'", "''")}')" }})
            """.trimIndent()
            val result = jaSync.executeQuery(query)
            for (row in result.rows) {
                val usernameVal = row.getString("username") ?: continue
                if (visitedUsernames.add(usernameVal)) {
                    val registeredDateObj = row["registered_date"]
                    val registeredDate = when (registeredDateObj) {
                        is String -> registeredDateObj
                        is OffsetDateTime -> registeredDateObj.toString()
                        else -> registeredDateObj?.toString() ?: ""
                    }
                    val uuidsJson = row.getString("uuids") ?: "[]"
                    val uuids = parseJsonArray(uuidsJson)
                    val javaUuids = uuids.filter { it.length == 36 }
                    val xboxIds = uuids.filter { it.length != 36 }
                    val ipJson = row.getString("ip_address") ?: "[]"
                    val ipAddresses = parseJsonArray(ipJson)
                    val country = row.getString("country") ?: ""
                    val regionName = row.getString("region") ?: ""
                    accounts.add(
                        AccountInfo(
                            username = usernameVal,
                            registeredDate = registeredDate,
                            javaUuids = javaUuids,
                            xboxIds = xboxIds,
                            country = country,
                            regionName = regionName,
                            ipAddresses = ipAddresses
                        )
                    )
                    visitedUuids.addAll(uuids)
                    visitedIps.addAll(ipAddresses)
                }
            }
        }

        searchAndCollect(setOf(name))

        var foundNew: Boolean
        do {
            foundNew = false
            val uuidQuery = if (visitedUuids.isNotEmpty()) {
                """
                SELECT username, registered_date, uuids, ip_address, country, region
                FROM accounts
                WHERE ${visitedUuids.joinToString(" OR ") { "'$it' = ANY (SELECT jsonb_array_elements_text(uuids))" }}
                """.trimIndent()
            } else null
            val ipQuery = if (visitedIps.isNotEmpty()) {
                """
                SELECT username, registered_date, uuids, ip_address, country, region
                FROM accounts
                WHERE ${visitedIps.joinToString(" OR ") { "'$it' = ANY (SELECT jsonb_array_elements_text(ip_address))" }}
                """.trimIndent()
            } else null

            val newUsernames = mutableSetOf<String>()
            if (uuidQuery != null) {
                val uuidResult = jaSync.executeQuery(uuidQuery)
                for (row in uuidResult.rows) {
                    val usernameVal = row.getString("username") ?: continue
                    if (!visitedUsernames.contains(usernameVal)) {
                        newUsernames.add(usernameVal)
                    }
                }
            }
            if (ipQuery != null) {
                val ipResult = jaSync.executeQuery(ipQuery)
                for (row in ipResult.rows) {
                    val usernameVal = row.getString("username") ?: continue
                    if (!visitedUsernames.contains(usernameVal)) {
                        newUsernames.add(usernameVal)
                    }
                }
            }
            if (newUsernames.isNotEmpty()) {
                searchAndCollect(newUsernames)
                foundNew = true
            }
        } while (foundNew)

        return accounts
    }

    private fun parseJsonArray(json: String): List<String> {
        return try {
            val arr = org.json.JSONArray(json)
            List(arr.length()) { arr.getString(it) }
        } catch (_: Exception) {
            emptyList()
        }
    }
    
    /**
     * Checks if the account has the specified permission.
     * First checks permissions from ranks (including inherited ones), then falls back to direct permissions.
     */
    suspend fun checkPermission(
        username: String,
        permission: String,
        lettuce: Lettuce
    ): Boolean {
        val cacheFallback = CacheFallback(lettuce, jaSync, logger)
        
        // Step 1: Get the player's ranks from Redis cache or DB fallback - using bulk fetch
        val userData = cacheFallback.getBulkData(username, listOf("ranks", "permissions"))

        // Step 2: Check if any of the player's ranks have this permission
        val ranksList = userData["ranks"]?.let { cacheFallback.parseJsonToList(it) } ?: emptyList()

        if (ranksList.isNotEmpty()) {
            // Extract all rank names, filtering out expired ranks
            val rankNames = getRankNamesWithExpiration(ranksList)

            // Check all ranks for the permission in one batch
            val allRankPerms = mutableSetOf<String>()
            for (rankName in rankNames) {
                // Use the cached rank permissions to avoid database lookups
                allRankPerms.addAll(jaSync.clerk.ranks.getAllPermissionsForRank(rankName))
            }

            // Check for exact permission match
            if (permission in allRankPerms) {
                return true
            }

            // Check for wildcard permissions at different levels
            // Example: clerk.* should match clerk.permission.add
            // Example: clerk.permission.* should match clerk.permission.add
            val permParts = permission.split(".")
            for (wildcard in allRankPerms.filter { it.contains("*") }) {
                // Case 1: Direct wildcard (e.g., "clerk.*")
                if (wildcard.endsWith(".*")) {
                    val basePermission = wildcard.removeSuffix(".*")
                    if (permission.startsWith("$basePermission.")) {
                        return true
                    }
                }

                // Case 2: Multi-level wildcard matching (e.g., "clerk.permission.*" should match "clerk.permission.add.permanent")
                val wildcardParts = wildcard.split(".")
                if (wildcardParts.size <= permParts.size) {
                    var matches = true
                    for (i in wildcardParts.indices) {
                        if (wildcardParts[i] == "*") {
                            // Wildcard matches rest of permission
                            return true
                        } else if (wildcardParts[i] != permParts[i]) {
                            matches = false
                            break
                        }
                    }

                    // All parts before wildcard match, now check if wildcard is at the end
                    if (matches && wildcardParts.last() == "*") {
                        return true
                    }
                }
            }
        }

        // Step 3: If no rank has the permission, check direct permissions
        val permsList = userData["permissions"]?.let { cacheFallback.parseJsonToList(it) } ?: emptyList()

        // Check for the permission
        val now = Instant.now()
        for (perm in permsList) {
            when (perm) {
                is String -> if (perm == permission) return true
                is Map<*, *> -> {
                    val permName = perm["permission"]?.toString()
                    val expires = perm["expires"]?.toString()?.toLongOrNull()
                    if (permName == permission) {
                        if (expires == null) return true
                        val expireTime = Instant.ofEpochSecond(expires)
                        if (now.isBefore(expireTime)) return true
                    }
                }
            }
        }
        return false
    }
    
    /**
     * Helper method to extract rank names from the ranks list, filtering out expired ranks
     */
    private fun getRankNamesWithExpiration(ranks: List<Any>): List<String> {
        val rankNames = mutableListOf<String>()
        val now = Instant.now()

        for (rank in ranks) {
            when (rank) {
                is String -> rankNames.add(rank)
                is Map<*, *> -> {
                    val rankName = rank["rank"]?.toString()
                    val expires = rank["expires"]?.toString()?.toLongOrNull()

                    if (rankName != null) {
                        if (expires == null || now.isBefore(Instant.ofEpochSecond(expires))) {
                            rankNames.add(rankName)
                        }
                    }
                }
            }
        }
        return rankNames
    }

    /**
     * Adds or removes a permission.
     */
    suspend fun modifyPermission(
        username: String,
        permission: String,
        add: Boolean,
        lettuce: Lettuce
    ): Boolean {
        val cacheFallback = CacheFallback(lettuce, jaSync, logger)
        lettuce.notifyAccountUpdate(username)
        
        return cacheFallback.updateData(
            username = username,
            column = "permissions",
            parser = { raw -> cacheFallback.parseJsonToList(raw).toMutableList() },
            modifier = { permsList ->
                val mutableList = permsList ?: mutableListOf()
                
                // Check if permission exists
                val now = Instant.now()
                val exists = mutableList.any {
                    when (it) {
                        is String -> it == permission
                        is Map<*, *> -> {
                            val permName = it["permission"]?.toString()
                            val expires = it["expires"]?.toString()?.toLongOrNull()
                            permName == permission && (expires == null || 
                                now.isBefore(Instant.ofEpochSecond(expires)))
                        }
                        else -> false
                    }
                }
                
                // Add or remove permission
                if (add && !exists) {
                    mutableList.add(permission)
                } else if (!add && exists) {
                    mutableList.removeIf {
                        when (it) {
                            is String -> it == permission
                            is Map<*, *> -> it["permission"]?.toString() == permission
                            else -> false
                        }
                    }
                }

                
                mutableList
            },
            serializer = { list -> 
                com.fasterxml.jackson.module.kotlin.jacksonObjectMapper().writeValueAsString(list) 
            }
        )
    }
    
    /**
     * Lists all permissions for an account.
     */
    suspend fun listPermissions(
        username: String,
        lettuce: Lettuce
    ): List<String> {
        val cacheFallback = CacheFallback(lettuce, jaSync, logger)
        
        val permsList = cacheFallback.getData(username, "permissions") { raw ->
            cacheFallback.parseJsonToList(raw)
        } ?: return emptyList()
        
        // Format permissions with expiration information
        val now = Instant.now()
        return permsList.map {
            when (it) {
                is String -> it
                is Map<*, *> -> {
                    val perm = it["permission"]?.toString() ?: ""
                    val expires = it["expires"]?.toString()?.toLongOrNull()
                    if (expires != null) {
                        val expireTime = Instant.ofEpochSecond(expires)
                        if (now.isAfter(expireTime)) {
                            "$perm (expired)"
                        } else {
                            "$perm (expires in ${cacheFallback.formatDuration(expires)})"
                        }
                    } else {
                        perm
                    }
                }
                else -> it.toString()
            }
        }
    }
    
    /**
     * Adds a timed permission with an expiration date.
     */
    suspend fun addTimedPermission(
        username: String,
        timedPermission: Map<String, Any>,
        lettuce: Lettuce
    ): Boolean {
        val cacheFallback = CacheFallback(lettuce, jaSync, logger)
        
        return cacheFallback.updateData(
            username = username,
            column = "permissions",
            parser = { raw -> cacheFallback.parseJsonToList(raw).toMutableList() },
            modifier = { permsList ->
                val mutableList = permsList ?: mutableListOf()
                
                // Check if permission already exists
                val permName = timedPermission["permission"]?.toString() ?: return@updateData null
                val now = Instant.now()
                val exists = mutableList.any {
                    when (it) {
                        is String -> it == permName
                        is Map<*, *> -> {
                            val existingPerm = it["permission"]?.toString()
                            val expires = it["expires"]?.toString()?.toLongOrNull()
                            existingPerm == permName && (expires == null || 
                                now.isBefore(Instant.ofEpochSecond(expires)))
                        }
                        else -> false
                    }
                }
                
                if (!exists) {
                    // Add timed permission
                    mutableList.add(timedPermission)
                }
                
                mutableList
            },
            serializer = { list -> 
                com.fasterxml.jackson.module.kotlin.jacksonObjectMapper().writeValueAsString(list) 
            }
        )
    }

    /**
     * Helper function to format time durations nicely
     */
    private fun formatDuration(duration: java.time.Duration, useAgo: Boolean = false): String {
        val days = duration.toDays()
        val hours = duration.toHoursPart()
        val minutes = duration.toMinutesPart()

        val formatted = when {
            days > 0 -> "${days}d ${hours}h"
            hours > 0 -> "${hours}h ${minutes}m"
            minutes > 0 -> "${minutes}m"
            else -> "moments"
        }

        return if (useAgo) "$formatted ago" else formatted
    }

    /**
     * Helper to update Redis requests cache
     */
    private suspend fun updateRedisRequestsCache(username: String, requestsType: String, requestData: Map<String, Any>, lettuce: Lettuce) {
        try {
            val cachedJson = withContext(Dispatchers.IO) {
                lettuce.getAccountCache(username)
            } ?: return

            val mapper = com.fasterxml.jackson.module.kotlin.jacksonObjectMapper()
            val accountData = mapper.readValue(cachedJson, MutableMap::class.java) as MutableMap<String, Any?>

            // Get existing requests list
            val requestsJson = accountData[requestsType]?.toString() ?: "[]"
            val requestsList = if (requestsJson.startsWith("[")) {
                try {
                    mapper.readValue(requestsJson, MutableList::class.java) as MutableList<Any>
                } catch (e: Exception) {
                    mutableListOf<Any>()
                }
            } else {
                mutableListOf<Any>()
            }

            // Check if request already exists
            val alreadyExists = requestsList.any {
                when (it) {
                    is Map<*, *> -> {
                        if (requestsType == "incoming_requests" && it.containsKey("from")) {
                            it["from"]?.toString()?.equals(requestData["from"]?.toString(), ignoreCase = true) == true
                        } else if (requestsType == "outgoing_requests" && it.containsKey("to")) {
                            it["to"]?.toString()?.equals(requestData["to"]?.toString(), ignoreCase = true) == true
                        } else {
                            false
                        }
                    }
                    else -> false
                }
            }

            // Add request if not already exists
            if (!alreadyExists) {
                requestsList.add(requestData)

                // Update the account data
                accountData[requestsType] = mapper.writeValueAsString(requestsList)

                // Save back to Redis
                val updatedJson = mapper.writeValueAsString(accountData)
                withContext(Dispatchers.IO) {
                    lettuce.connection?.sync()?.setex("account:$username", 3600, updatedJson)
                }
            }
        } catch (e: Exception) {
            logger.warn(Component.text("Failed to update Redis requests cache: ${e.message}", NamedTextColor.YELLOW))
        }
    }

    /**
     * Helper to update Redis requests data directly (full replacement)
     */
    private suspend fun updateRedisRequestsData(username: String, requestsType: String, requestsJson: String, lettuce: Lettuce) {
        try {
            val cachedJson = withContext(Dispatchers.IO) {
                lettuce.getAccountCache(username)
            } ?: return

            val mapper = com.fasterxml.jackson.module.kotlin.jacksonObjectMapper()
            val accountData = mapper.readValue(cachedJson, MutableMap::class.java) as MutableMap<String, Any?>

            // Update the requests data
            accountData[requestsType] = requestsJson

            // Save back to Redis
            val updatedJson = mapper.writeValueAsString(accountData)
            withContext(Dispatchers.IO) {
                lettuce.connection?.sync()?.setex("account:$username", 3600, updatedJson)
            }
        } catch (e: Exception) {
            logger.warn(Component.text("Failed to update Redis requests data: ${e.message}", NamedTextColor.YELLOW))
        }
    }

    /**
     * Helper to remove a request from Redis cache
     */
    private suspend fun removeRedisRequestCache(username: String, requestsType: String, fieldName: String, fieldValue: String, lettuce: Lettuce) {
        try {
            val cachedJson = withContext(Dispatchers.IO) {
                lettuce.getAccountCache(username)
            } ?: return

            val mapper = com.fasterxml.jackson.module.kotlin.jacksonObjectMapper()
            val accountData = mapper.readValue(cachedJson, MutableMap::class.java) as MutableMap<String, Any?>

            // Get existing requests list
            val requestsJson = accountData[requestsType]?.toString() ?: return
            if (!requestsJson.startsWith("[")) return

            try {
                val requestsList = mapper.readValue(requestsJson, MutableList::class.java)

                // Remove the request
                val updatedList = requestsList.filterNot {
                    when (it) {
                        is Map<*, *> -> it[fieldName]?.toString()?.equals(fieldValue, ignoreCase = true) == true
                        else -> false
                    }
                }

                // Update the account data
                accountData[requestsType] = mapper.writeValueAsString(updatedList)

                // Save back to Redis
                val updatedJson = mapper.writeValueAsString(accountData)
                withContext(Dispatchers.IO) {
                    lettuce.connection?.sync()?.setex("account:$username", 3600, updatedJson)
                }
            } catch (e: Exception) {
                logger.warn(Component.text("Error parsing Redis requests data: ${e.message}", NamedTextColor.YELLOW))
            }
        } catch (e: Exception) {
            logger.warn(Component.text("Failed to update Redis requests cache: ${e.message}", NamedTextColor.YELLOW))
        }
    }

    /**
     * Helper to clear all requests between two users in Redis cache
     */
    private suspend fun clearRedisRequestsCaches(username: String, target: String, lettuce: Lettuce) {
        try {
            // Clear username's requests
            removeRedisRequestCache(username, "incoming_requests", "from", target, lettuce)
            removeRedisRequestCache(username, "outgoing_requests", "to", target, lettuce)

            // Clear target's requests
            removeRedisRequestCache(target, "incoming_requests", "from", username, lettuce)
            removeRedisRequestCache(target, "outgoing_requests", "to", username, lettuce)
        } catch (e: Exception) {
            logger.warn(Component.text("Failed to clear Redis requests cache: ${e.message}", NamedTextColor.YELLOW))
        }
    }

    /**
     * Updates a setting in the user's settings column
     * Uses Redis caching with Lettuce to minimize database calls
     *
     * @param username The username of the account
     * @param settingKey The key of the setting to update
     * @param value The new value for the setting (null to remove the setting)
     * @param lettuce The Lettuce instance for Redis caching
     * @return The new value of the setting, or null if operation failed
     */
    suspend fun updateSetting(
        username: String,
        settingKey: String,
        value: Any?,
        lettuce: Lettuce
    ): Any? {
        val cacheFallback = CacheFallback(lettuce, jaSync, logger)

        return cacheFallback.updateData(
            username = username,
            column = "settings",
            parser = { raw ->
                try {
                    if (raw == null || raw == "[]" || raw == "") {
                        mutableMapOf<String, Any>()
                    } else if (raw is String) {
                        val mapper = com.fasterxml.jackson.module.kotlin.jacksonObjectMapper()
                        mapper.readValue(raw, Map::class.java) as MutableMap<String, Any>
                    } else if (raw is Map<*, *>) {
                        raw as MutableMap<String, Any>
                    } else {
                        mutableMapOf<String, Any>()
                    }
                } catch (e: Exception) {
                    logger.warn(Component.text(
                        "Error parsing settings JSON: ${e.message}",
                        NamedTextColor.YELLOW
                    ))
                    mutableMapOf<String, Any>()
                }
            },
            modifier = { settings ->
                val mutableSettings = settings ?: mutableMapOf()

                if (value == null && settingKey in mutableSettings) {
                    // Remove the setting if value is null
                    mutableSettings.remove(settingKey)
                } else if (value != null) {
                    // Update or add the setting
                    mutableSettings[settingKey] = value
                }

                mutableSettings
            },
            serializer = { settings ->
                com.fasterxml.jackson.module.kotlin.jacksonObjectMapper().writeValueAsString(settings)
            }
        ).let { success ->
            if (success) {
                // Return the new value
                return@let value
            }
            null
        }
    }

    /**
     * Gets a setting value from the user's settings column
     * Uses Redis caching with Lettuce to minimize database calls
     *
     * @param username The username of the account
     * @param settingKey The key of the setting to retrieve
     * @param defaultValue The default value to return if setting doesn't exist
     * @param lettuce The Lettuce instance for Redis caching
     * @return The value of the setting, or the default value if not found
     */
    suspend fun getSetting(
        username: String,
        settingKey: String,
        defaultValue: Any?,
        lettuce: Lettuce
    ): Any? {
        val cacheFallback = CacheFallback(lettuce, jaSync, logger)

        val settings = cacheFallback.getData(
            username = username,
            column = "settings",
            transform = { raw ->
                try {
                    if (raw == null || raw == "[]" || raw == "") {
                        emptyMap<String, Any>()
                    } else if (raw is String) {
                        val mapper = com.fasterxml.jackson.module.kotlin.jacksonObjectMapper()
                        mapper.readValue(raw, Map::class.java)
                    } else if (raw is Map<*, *>) {
                        raw
                    } else {
                        emptyMap<String, Any>()
                    }
                } catch (e: Exception) {
                    logger.warn(Component.text(
                        "Error parsing settings JSON: ${e.message}",
                        NamedTextColor.YELLOW
                    ))
                    emptyMap<String, Any>()
                }
            }
        ) ?: return defaultValue

        return if (settingKey in settings) settings[settingKey] else defaultValue
    }

    /**
     * Toggle a boolean setting in the user's settings column
     * Uses Redis caching with Lettuce to minimize database calls
     *
     * @param username The username of the account
     * @param settingKey The key of the setting to toggle
     * @param lettuce The Lettuce instance for Redis caching
     * @return The new value of the setting after toggling, or null if operation failed
     */
    suspend fun toggleSetting(
        username: String,
        settingKey: String,
        lettuce: Lettuce
    ): Boolean? {
        // Get the current value first
        val currentValue = getSetting(username, settingKey, false, lettuce) as? Boolean ?: false

        // Toggle and update the setting
        val newValue = !currentValue
        return updateSetting(username, settingKey, newValue, lettuce) as? Boolean
    }
}
