package twizzy.tech.clerk.player

import com.github.jasync.sql.db.RowData
import com.velocitypowered.api.proxy.Player
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.geysermc.floodgate.api.FloodgateApi
import org.json.JSONObject
import twizzy.tech.clerk.util.PostSchema
import java.net.URL
import kotlin.toString
import net.kyori.adventure.text.Component
import net.kyori.adventure.text.format.NamedTextColor
import net.kyori.adventure.text.logger.slf4j.ComponentLogger
import twizzy.tech.clerk.util.JaSync
import java.time.OffsetDateTime
import kotlin.text.get

class Account(private val jaSync: JaSync, private val logger: ComponentLogger) {

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
        details["registered_date"] = java.time.Instant.now().toString()

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
        val now = java.time.Instant.now().toString()

        val checkLockAndCredentialsQuery = """
            SELECT 
                username, 
                locked, 
                lock_until, 
                lock_reason, 
                failed_attempts,
                (password = crypt('${password.replace("'", "''")}', password)) as password_match
            FROM accounts 
            WHERE username = '${username.replace("'", "''")}';
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
        
        val currentTime = java.time.OffsetDateTime.now()
        
        if (locked && (lockUntil == null || currentTime.isBefore(lockUntil))) {
            val lockMessage = if (lockUntil != null) {
                "Your account is locked until ${lockUntil.format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))}. Reason: $lockReason"
            } else {
                "Your account is permanently locked. Reason: $lockReason. Please contact an administrator."
            }
            player.sendMessage(Component.text(lockMessage, NamedTextColor.RED))
            return false
        } else if (locked && lockUntil != null && currentTime.isAfter(lockUntil)) {
            val unlockQuery = """
                UPDATE accounts 
                SET locked = FALSE, lock_until = NULL, lock_reason = '' 
                WHERE username = '${username.replace("'", "''")}';
            """.trimIndent()
            jaSync.executeQuery(unlockQuery)
        }

        if (passwordMatch) {
            val accountQuery = """
                SELECT country, region FROM accounts
                WHERE username = '${username.replace("'", "''")}';
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
                        WHERE username = '${username.replace("'", "''")}';
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
                            WHERE username = '${username.replace("'", "''")}';
                        """.trimIndent()
                        jaSync.executeQuery(lockQuery)
                        
                        player.sendMessage(Component.text(
                            "Security alert: Login from a different region via proxy detected. Your account has been locked.",
                            NamedTextColor.RED
                        ))
                        return false
                    }
                }
            }

            val updates = listOf(
                """
                UPDATE accounts 
                SET failed_attempts = 0, 
                    locked = FALSE, 
                    lock_until = NULL, 
                    lock_reason = '' 
                WHERE username = '${username.replace("'", "''")}';
                """.trimIndent(),
                
                """
                UPDATE accounts
                SET logins = COALESCE(logins, '[]'::jsonb) || jsonb_build_object(
                    'id', '$uuidOrXuid',
                    'platform', '$platform',
                    'ip_address', '$ip',
                    'date', '$now'
                ),
                logged_out = FALSE
                WHERE username = '${username.replace("'", "''")}';
                """.trimIndent()
            )
            
            jaSync.executeBatch(updates)
            
            return true
        } else {
            val usernameExistsQuery = """
                SELECT 1 FROM accounts
                WHERE username = '${username.replace("'", "''")}';
            """.trimIndent()
            val usernameExists = jaSync.executeQuery(usernameExistsQuery).rows.isNotEmpty()
            
            if (usernameExists) {
                val incrementQuery = """
                    UPDATE accounts 
                    SET failed_attempts = failed_attempts + 1 
                    WHERE username = '${username.replace("'", "''")}';
                """.trimIndent()
                jaSync.executeQuery(incrementQuery)
                
                val attemptsQuery = """
                    SELECT failed_attempts FROM accounts 
                    WHERE username = '${username.replace("'", "''")}';
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
                            val lockUntil = java.time.OffsetDateTime.now().plusMinutes(5)
                            val lockQuery = """
                                UPDATE accounts 
                                SET locked = TRUE, 
                                    lock_until = '${lockUntil}', 
                                    lock_reason = 'Too many failed login attempts (5). Account locked for 5 minutes.' 
                                WHERE username = '${username.replace("'", "''")}';
                            """.trimIndent()
                            jaSync.executeQuery(lockQuery)
                            player.sendMessage(Component.text("Account locked for 5 minutes due to too many failed login attempts.", NamedTextColor.RED))
                            player.currentServer.ifPresent { server ->
                                logger?.info(
                                    Component.text("Account $username locked for 5 minutes due to failed attempts.", NamedTextColor.RED)
                                )
                            }
                        }
                        10 -> {
                            val lockUntil = java.time.OffsetDateTime.now().plusMinutes(30)
                            val lockQuery = """
                                UPDATE accounts 
                                SET locked = TRUE, 
                                    lock_until = '${lockUntil}', 
                                    lock_reason = 'Too many failed login attempts (10). Account locked for 30 minutes.' 
                                WHERE username = '${username.replace("'", "''")}';
                            """.trimIndent()
                            jaSync.executeQuery(lockQuery)
                            player.sendMessage(Component.text("Account locked for 30 minutes due to too many failed login attempts.", NamedTextColor.RED))
                            player.currentServer.ifPresent { server ->
                                logger?.info(
                                    Component.text("Account $username locked for 30 minutes due to failed attempts.", NamedTextColor.RED)
                                )
                            }
                        }
                        15 -> {
                            val lockUntil = java.time.OffsetDateTime.now().plusHours(1)
                            val lockQuery = """
                                UPDATE accounts 
                                SET locked = TRUE, 
                                    lock_until = '${lockUntil}', 
                                    lock_reason = 'Too many failed login attempts (15). Account locked for 1 hour.' 
                                WHERE username = '${username.replace("'", "''")}';
                            """.trimIndent()
                            jaSync.executeQuery(lockQuery)
                            player.sendMessage(Component.text("Account locked for 1 hour due to too many failed login attempts.", NamedTextColor.RED))
                            player.currentServer.ifPresent { server ->
                                logger?.info(
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
                                WHERE username = '${username.replace("'", "''")}';
                            """.trimIndent()
                            jaSync.executeQuery(lockQuery)
                            player.sendMessage(Component.text("Account permanently locked due to excessive failed login attempts. Please contact an administrator.", NamedTextColor.RED))
                            player.currentServer.ifPresent { server ->
                                logger?.info(
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
        } catch (e: Exception) {
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
        } catch (e: Exception) {
            null
        }
    }
    
    private fun RowData.getTimestampOrNull(columnName: String): java.time.OffsetDateTime? {
        return try {
            getAs<java.time.OffsetDateTime>(columnName)
        } catch (e: Exception) {
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
            WHERE username = '$username';
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
                WHERE username IN ($inClause)
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
        } catch (e: Exception) {
            emptyList()
        }
    }
}
