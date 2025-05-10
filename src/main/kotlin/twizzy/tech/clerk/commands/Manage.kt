package twizzy.tech.clerk.commands

import com.velocitypowered.api.proxy.Player
import kotlinx.coroutines.runBlocking
import net.kyori.adventure.text.Component
import net.kyori.adventure.text.format.NamedTextColor
import revxrsal.commands.annotation.Command
import revxrsal.commands.annotation.Subcommand
import revxrsal.commands.annotation.Optional
import revxrsal.commands.velocity.annotation.CommandPermission
import twizzy.tech.clerk.Clerk
import twizzy.tech.clerk.commands.Register
import twizzy.tech.clerk.player.Account
import twizzy.tech.clerk.util.JaSync
import kotlin.collections.get
import kotlin.text.append

@Command("account")
class Manage(private val clerk: Clerk, private val jaSync: JaSync) {

    // Store password reset state: Player -> (stage, oldPassword)
    private val pendingPasswordResets = mutableMapOf<Player, Pair<ResetStage, String?>>()

    private enum class ResetStage { WAITING_FOR_OLD, WAITING_FOR_NEW }

    @Subcommand("reset")
    suspend fun reset(actor: Player) {
        if (pendingPasswordResets.containsKey(actor)) {
            actor.sendMessage(Component.text("You are already in the process of resetting your password. Please follow the prompts in chat.", NamedTextColor.YELLOW))
            return
        }
        pendingPasswordResets[actor] = ResetStage.WAITING_FOR_OLD to null
        actor.sendMessage(Component.text("Please type your current password in chat to begin the reset process. Type 'cancel' to cancel.", NamedTextColor.YELLOW))
    }

    // Call this from your chat event handler in Clerk.kt
    fun handlePasswordResetChat(actor: Player, message: String) {
        val state = pendingPasswordResets[actor] ?: return

        // Cancel logic
        if (message.equals("cancel", ignoreCase = true)) {
            pendingPasswordResets.remove(actor)
            actor.sendMessage(Component.text("Password reset cancelled.", NamedTextColor.RED))
            return
        }

        val username = actor.username

        when (state.first) {
            ResetStage.WAITING_FOR_OLD -> {
                // Validate old password
                val validOld = runBlocking {
                    val query = """
                        SELECT 1 FROM accounts
                        WHERE username = '${username.replace("'", "''")}'
                        AND password = crypt('${message.replace("'", "''")}', password)
                        LIMIT 1;
                    """.trimIndent()
                    jaSync.executeQuery(query).rows.isNotEmpty()
                }
                if (!validOld) {
                    actor.sendMessage(Component.text("Your current password is incorrect. Password reset cancelled.", NamedTextColor.RED))
                    pendingPasswordResets.remove(actor)
                    return
                }
                pendingPasswordResets[actor] = ResetStage.WAITING_FOR_NEW to message
                actor.sendMessage(Component.text("Please type your new password in chat. Type 'cancel' to cancel.", NamedTextColor.YELLOW))
            }
            ResetStage.WAITING_FOR_NEW -> {
                val oldPassword = state.second!!
                val newPassword = message
                // Password validation
                if (newPassword.length < 6) {
                    actor.sendMessage(Component.text("Your password must be at least 6 characters long.", NamedTextColor.RED))
                    pendingPasswordResets.remove(actor)
                    return
                }
                if (
                    !newPassword.any { it.isUpperCase() } ||
                    !newPassword.any { it.isLowerCase() } ||
                    !newPassword.any { it.isDigit() || !it.isLetterOrDigit() }
                ) {
                    actor.sendMessage(Component.text("Your password is too weak, try making it more complex.", NamedTextColor.RED))
                    pendingPasswordResets.remove(actor)
                    return
                }
                if (oldPassword == newPassword) {
                    actor.sendMessage(Component.text("The new password must be different from the old password.", NamedTextColor.RED))
                    pendingPasswordResets.remove(actor)
                    return
                }
                // Actually update the password in the database
                runBlocking {
                    val query = """
                        UPDATE accounts
                        SET password = crypt('${newPassword.replace("'", "''")}', gen_salt('bf'))
                        WHERE username = '${username.replace("'", "''")}'
                    """.trimIndent()
                    jaSync.executeQuery(query)
                }
                actor.disconnect(Component.text("Your password has been changed successfully. You have been logged out for security reasons.", NamedTextColor.GREEN))
                clerk.logger.info(Component.text("$username has just updated their password.", NamedTextColor.GREEN))
                runBlocking { Account(jaSync, clerk.logger).setLoggedOut(username, true) }
                pendingPasswordResets.remove(actor)
            }
        }
    }

    @Subcommand("logins")
    suspend fun logins(actor: Player) {
        val username = actor.username

        val query = """
        SELECT logins FROM accounts
        WHERE username = '${username.replace("'", "''")}'
        """.trimIndent()
        val result = jaSync.executeQuery(query)
        if (result.rows.isEmpty()) {
            actor.sendMessage(Component.text("No login history found.", NamedTextColor.YELLOW))
            return
        }
        val loginsJson = result.rows[0].getString("logins") ?: "[]"
        val logins = try {
            val arr = org.json.JSONArray(loginsJson)
            (0 until arr.length()).map { arr.getJSONObject(it) }
        } catch (e: Exception) {
            emptyList<org.json.JSONObject>()
        }
        if (logins.isEmpty()) {
            actor.sendMessage(Component.text("No login history found.", NamedTextColor.YELLOW))
            return
        }
        actor.sendMessage(Component.text("Last 10 logins:", NamedTextColor.GREEN))
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
            val msg = Component.text("$ago on $platform from $country, $region", NamedTextColor.AQUA)
            actor.sendMessage(msg)
        }
    }

    @Subcommand("autolock")
    suspend fun autolock(actor: Player) {
        val username = actor.username

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
            actor.sendMessage(Component.text("Auto-lock enabled. You will be logged out every time you disconnect.", NamedTextColor.GREEN))
        } else {
            actor.sendMessage(Component.text("Auto-lock disabled. You will stay logged in after disconnecting.", NamedTextColor.GREEN))
        }
    }
}

