package twizzy.tech.clerk.commands

import com.velocitypowered.api.proxy.Player
import kotlinx.coroutines.launch
import net.kyori.adventure.text.Component
import net.kyori.adventure.text.format.NamedTextColor
import net.kyori.adventure.text.format.TextDecoration
import revxrsal.commands.annotation.Command
import revxrsal.commands.annotation.Cooldown
import revxrsal.commands.annotation.Subcommand
import revxrsal.commands.annotation.Optional
import revxrsal.commands.velocity.annotation.CommandPermission
import twizzy.tech.clerk.Clerk
import twizzy.tech.clerk.player.Account
import twizzy.tech.clerk.util.JacksonFactory
import twizzy.tech.clerk.util.JaSync
import kotlin.collections.get

@Command("account")
class Manage(private val clerk: Clerk) {

    private val account = clerk.account
    private val jaSync = clerk.jaSync
    private val langConfig = JacksonFactory.loadLangConfig()

    companion object {
        // Store password reset state globally, so it persists across instances
        val pendingPasswordResets = mutableMapOf<Player, Pair<String, String?>>()
    }

    @Command("account")
    fun accountUsage(actor: Player) {
        // Get the multi-line help message from the language configuration
        val helpMessage = langConfig.getMessage("account.usage.help")

        // Split the help message by lines and process each line
        val helpLines = helpMessage.lines()

        // Add the top divider
        actor.sendMessage(Component.text("                                                                              ", NamedTextColor.DARK_GREEN, TextDecoration.STRIKETHROUGH))

        // Process each line of the help message
        helpLines.forEach { line ->
            // Remove YAML list indicator (hyphen) if present and trim whitespace
            val cleanLine = if (line.startsWith("-")) line.substring(1).trim() else line.trim()
            if (cleanLine.isNotEmpty()) {
                actor.sendMessage(Component.text(cleanLine))
            }
        }

        // Add the bottom divider
        actor.sendMessage(Component.text("                                                                              ", NamedTextColor.DARK_GREEN, TextDecoration.STRIKETHROUGH))
    }

    @Subcommand("reset")
    suspend fun reset(actor: Player) {
        if (pendingPasswordResets.containsKey(actor)) {
            actor.sendMessage(Component.text(langConfig.getMessage("account.reset.already_in_process")))
            return
        }

        // Start the reset process
        pendingPasswordResets[actor] = "old" to null
        actor.sendMessage(Component.text(langConfig.getMessage("account.reset.start")))
    }

    fun handlePasswordResetChat(actor: Player, message: String) {
        val state = pendingPasswordResets[actor] ?: return
        val username = actor.username

        // Cancel logic
        if (message.equals("cancel", ignoreCase = true)) {
            pendingPasswordResets.remove(actor)
            actor.sendMessage(Component.text(langConfig.getMessage("account.reset.cancelled")))
            return
        }

        when (state.first) {
            "old" -> {
                // Validate old password
                clerk.scope.launch {
                    val query = """
                        SELECT 1 FROM accounts
                        WHERE username = '${username.replace("'", "''")}'
                        AND password = crypt('${message.replace("'", "''")}', password)
                        LIMIT 1;
                    """.trimIndent()
                    val result = jaSync.executeQuery(query)
                    val validOld = result.rows.isNotEmpty()

                    if (!validOld) {
                        actor.sendMessage(Component.text(langConfig.getMessage("account.reset.incorrect_password")))
                        // Keep the player in the reset state instead of removing them
                        return@launch
                    }
                    pendingPasswordResets[actor] = "new" to message
                    actor.sendMessage(Component.text(langConfig.getMessage("account.reset.enter_new")))
                }
            }
            "new" -> {
                val oldPassword = state.second!!
                val newPassword = message

                // Password validation
                if (newPassword.length < 6) {
                    actor.sendMessage(Component.text(langConfig.getMessage("account.reset.password_too_short")))
                    // Keep the player in the reset state instead of removing them
                    return
                }
                if (
                    !newPassword.any { it.isUpperCase() } ||
                    !newPassword.any { it.isLowerCase() } ||
                    !newPassword.any { it.isDigit() || !it.isLetterOrDigit() }
                ) {
                    actor.sendMessage(Component.text(langConfig.getMessage("account.reset.password_too_weak")))
                    // Keep the player in the reset state instead of removing them
                    return
                }
                if (oldPassword == newPassword) {
                    actor.sendMessage(Component.text(langConfig.getMessage("account.reset.password_same")))
                    // Keep the player in the same state instead of removing them from the process
                    return
                }

                pendingPasswordResets[actor] = "confirm" to newPassword
                actor.sendMessage(Component.text(langConfig.getMessage("account.reset.confirm_new")))
            }
            "confirm" -> {
                val newPassword = state.second!!

                if (message != newPassword) {
                    actor.sendMessage(Component.text(langConfig.getMessage("account.reset.passwords_dont_match")))
                    pendingPasswordResets.remove(actor)
                    return
                }

                // Actually update the password in the database
                clerk.scope.launch {
                    val query = """
                        UPDATE accounts
                        SET password = crypt('${newPassword.replace("'", "''")}', gen_salt('bf'))
                        WHERE username = '${username.replace("'", "''")}'
                    """.trimIndent()
                    jaSync.executeQuery(query)

                    actor.disconnect(Component.text(langConfig.getMessage("account.reset.success")))
                    clerk.logger.info(Component.text("$username has just updated their password.", NamedTextColor.GREEN))
                    account.setLoggedOut(username, true)
                    pendingPasswordResets.remove(actor)
                }
            }
        }
    }

    @Subcommand("logins")
    @Cooldown(10)
    fun logins(actor: Player) {
        val username = actor.username

        // Use MCCoroutine scope instead of blocking
        clerk.scope.launch {
            val query = """
            SELECT logins FROM accounts
            WHERE username = '${username.replace("'", "''")}'
            """.trimIndent()
            val result = jaSync.executeQuery(query)
            if (result.rows.isEmpty()) {
                actor.sendMessage(Component.text(langConfig.getMessage("account.logins.no_history")))
                return@launch
            }

            val loginsJson = result.rows[0].getString("logins") ?: "[]"
            val logins = try {
                val arr = org.json.JSONArray(loginsJson)
                (0 until arr.length()).map { arr.getJSONObject(it) }
            } catch (e: Exception) {
                emptyList<org.json.JSONObject>()
            }

            if (logins.isEmpty()) {
                actor.sendMessage(Component.text(langConfig.getMessage("account.logins.no_history")))
                return@launch
            }

            actor.sendMessage(Component.text(langConfig.getMessage("account.logins.header")))
            logins.takeLast(10).forEach { login ->
                val platform = login.optString("platform", "Java")
                val dateStr = login.optString("date", "")
                val country = login.optString("country", "Unknown")
                val region = login.optString("region", "Unknown")
                val ago = try {
                    val loginTime = java.time.OffsetDateTime.parse(dateStr)
                    val now = java.time.OffsetDateTime.now()
                    val duration = java.time.Duration.between(loginTime, now)
                    when {
                        duration.toDays() > 0 -> "${duration.toDays()}d ago"
                        duration.toHours() > 0 -> "${duration.toHours()}h ago"
                        duration.toMinutes() > 0 -> "${duration.toMinutes()}m ago"
                        else -> "just now"
                    }
                } catch (e: Exception) {
                    "unknown"
                }
                val msg = Component.text(langConfig.getMessage("account.logins.format", mapOf(
                    "ago" to ago,
                    "platform" to platform,
                    "country" to country,
                    "region" to region
                )))
                actor.sendMessage(msg)
            }
        }
    }

    @Subcommand("autolock")
    @Cooldown(10)
    fun autolock(actor: Player) {
        val username = actor.username

        // Use MCCoroutine scope instead of blocking
        clerk.scope.launch {
            // Check current auto_lock state
            val checkQuery = """
                SELECT auto_lock FROM accounts WHERE username = '${username.replace("'", "''")}' LIMIT 1;
            """.trimIndent()
            val result = jaSync.executeQuery(checkQuery)
            val currentAutoLock = result.rows.firstOrNull()?.getBoolean("auto_lock") ?: false

            val newAutoLock = !currentAutoLock
            val updateQuery = """
                UPDATE accounts
                SET auto_lock = ${if (newAutoLock) "TRUE" else "FALSE"}
                WHERE username = '${username.replace("'", "''")}'
            """.trimIndent()
            jaSync.executeQuery(updateQuery)

            if (newAutoLock) {
                actor.sendMessage(Component.text(langConfig.getMessage("account.autolock.enabled")))
            } else {
                actor.sendMessage(Component.text(langConfig.getMessage("account.autolock.disabled")))
            }
        }
    }
}
