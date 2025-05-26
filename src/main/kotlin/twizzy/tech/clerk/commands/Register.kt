package twizzy.tech.clerk.commands

import com.velocitypowered.api.proxy.Player
import kotlinx.coroutines.runBlocking
import net.kyori.adventure.text.Component
import net.kyori.adventure.text.format.NamedTextColor
import revxrsal.commands.annotation.Command
import revxrsal.commands.annotation.Cooldown
import revxrsal.commands.annotation.Optional
import twizzy.tech.clerk.Clerk
import twizzy.tech.clerk.player.Account
import twizzy.tech.clerk.util.CancelCulture
import twizzy.tech.clerk.util.JacksonFactory
import twizzy.tech.clerk.util.JaSync
import java.util.concurrent.TimeUnit

class Register(private val clerk: Clerk) {

    private val jaSync = clerk.jaSync
    private val account = clerk.account
    private val langConfig = JacksonFactory.loadLangConfig()

    companion object {
        // Store registration info as Pair<username, password> in a companion object
        // so it persists across all instances of Register
        val pendingRegistrations = mutableMapOf<Player, Pair<String, String>>()
    }

    @Command("register")
    suspend fun attemptRegister(actor: Player, @Optional username: String?, @Optional password: String?) {
        if (clerk.unauthenticatedPlayers.contains(actor.uniqueId)) {
            if (pendingRegistrations.containsKey(actor)) {
                actor.sendMessage(Component.text(langConfig.getMessage("register.already_started"), NamedTextColor.YELLOW))
                return
            }
            if (username.isNullOrEmpty() || password.isNullOrEmpty()) {
                actor.sendMessage(Component.text(langConfig.getMessage("register.usage"), NamedTextColor.YELLOW))
                return
            } else {
                val (valid, message) = validateEntry(username, password)
                if (!valid) {
                    actor.sendMessage(Component.text(message, NamedTextColor.RED))
                    return
                }
                clerk.awaitingPasswordConfirmation[actor] = password
                pendingRegistrations[actor] = username to password
                actor.sendMessage(Component.text(langConfig.getMessage("register.confirm_password"), NamedTextColor.YELLOW))
            }
        } else {
            actor.sendMessage(Component.text(langConfig.getMessage("register.already_logged_in"), NamedTextColor.RED))
        }
    }

    suspend fun validateEntry(username: String, password: String): Pair<Boolean, String> {

        // Username checks
        if (username.length !in 3..16) {
            return false to langConfig.getMessage("register.validate.username_length")
        }
        val allowedUsernameRegex = Regex("^[a-zA-Z0-9_.-]+$")
        if (!allowedUsernameRegex.matches(username)) {
            return false to langConfig.getMessage("register.validate.username_chars")
        }

        // CancelFilter check
        val cancelCulture = CancelCulture()
        if (!cancelCulture.isAllowed(username)) {
            return false to langConfig.getMessage("register.validate.username_inappropriate")
        }

        // Password checks
        if (password.length < 6) {
            return false to langConfig.getMessage("register.validate.password_length")
        }
        if (
            !password.any { it.isUpperCase() } ||
            !password.any { it.isLowerCase() } ||
            !password.any { it.isDigit() || !it.isLetterOrDigit() }
        ) {
            return false to langConfig.getMessage("register.validate.password_weak")
        }

        // Check if username is available using the passed jaSync instance
        val usernameExists = jaSync.executeQuery(
            "SELECT 1 FROM accounts WHERE username = '${username.replace("'", "''")}' LIMIT 1;"
        ).rows.isNotEmpty()
        if (usernameExists) {
            return false to langConfig.getMessage("register.validate.username_taken")
        }

        return true to ""
    }

    fun confirmRegistration(actor: Player, password: String) {
        val registration = pendingRegistrations.remove(actor)
        
        if (registration != null) {
            val passwordsMatch = registration.second == password
            
            if (passwordsMatch) {
                val username = registration.first
                
                // Call Account.registerAccount to gather details and then insert into the database
                runBlocking {
                    try {
                        val accountDetails = account.registerAccount(actor, username, password)
                        val result = jaSync.insertNewAccount(accountDetails)
                        
                        actor.sendMessage(Component.text(
                            langConfig.getMessage("register.successful", mapOf("username" to username)),
                            NamedTextColor.GREEN
                        ))
                        // Add green logger for registration
                        clerk.logger.info(Component.text("Player ${actor.username} registered a new account: $username", NamedTextColor.GREEN))
                    } catch (e: Exception) {
                        actor.sendMessage(Component.text(
                            langConfig.getMessage("register.failed", mapOf("error" to (e.message ?: "Unknown error"))),
                            NamedTextColor.RED
                        ))

                        // Clear registration state in case of error
                        pendingRegistrations.remove(actor)
                        clerk.awaitingPasswordConfirmation.remove(actor)
                    }
                }
            } else {
                // Clear any pending registration state for this player
                pendingRegistrations.remove(actor)
                clerk.awaitingPasswordConfirmation.remove(actor)
                actor.sendMessage(Component.text(langConfig.getMessage("register.passwords_not_match"), NamedTextColor.RED))
            }
        } else {
            // Clear any pending registration state for this player
            clerk.awaitingPasswordConfirmation.remove(actor)
            actor.sendMessage(Component.text(langConfig.getMessage("register.no_registration"), NamedTextColor.RED))
        }
    }
}
