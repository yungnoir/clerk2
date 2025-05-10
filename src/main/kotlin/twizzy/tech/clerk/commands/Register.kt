package twizzy.tech.clerk.commands

import com.velocitypowered.api.proxy.Player
import kotlinx.coroutines.runBlocking
import net.kyori.adventure.text.Component
import net.kyori.adventure.text.format.NamedTextColor
import revxrsal.commands.annotation.Command
import revxrsal.commands.annotation.Optional
import twizzy.tech.clerk.Clerk
import twizzy.tech.clerk.player.Account
import twizzy.tech.clerk.util.JaSync
import twizzy.tech.clerk.util.CancelCulture

class Register(private val clerk: Clerk, private val jaSync: JaSync) {

    // Store registration info as Pair<username, password>
    private val pendingRegistrations = mutableMapOf<Player, Pair<String, String>>()

    @Command("register")
    suspend fun attemptRegister(actor: Player, @Optional username: String?, @Optional password: String?) {
        if (clerk.unauthenticatedPlayers.contains(actor)) {
            if (pendingRegistrations.containsKey(actor)) {
                actor.sendMessage(Component.text("You have already started registration. Please type your password in chat to confirm.", NamedTextColor.YELLOW))
                return
            }
            if (username.isNullOrEmpty() || password.isNullOrEmpty()) {
                actor.sendMessage(Component.text("Usage: /register <username> <password>", NamedTextColor.YELLOW))
                return
            } else {
                val (valid, message) = validateEntry(username, password)
                if (!valid) {
                    actor.sendMessage(Component.text(message, NamedTextColor.RED))
                    return
                }
                clerk.awaitingPasswordConfirmation[actor] = password
                pendingRegistrations[actor] = username to password
                actor.sendMessage(Component.text("Please type your password in chat to confirm registration.", NamedTextColor.YELLOW))
            }
        } else {
            actor.sendMessage(Component.text("You are already logged into an account, please log out.", NamedTextColor.RED))
        }
    }

    suspend fun validateEntry(username: String, password: String): Pair<Boolean, String> {

        // Username checks
        if (username.length !in 3..12) {
            return false to "Your username must be between 3 and 12 characters."
        }
        val allowedUsernameRegex = Regex("^[a-zA-Z0-9_.-]+$")
        if (!allowedUsernameRegex.matches(username)) {
            return false to "Your username must be alphanumerical."
        }

        // CancelFilter check
        val cancelCulture = CancelCulture()
        if (!cancelCulture.isAllowed(username)) {
            return false to "Your username contains inappropriate or blocked words."
        }

        // Password checks
        if (password.length < 6) {
            return false to "Your password must be at least 6 characters long."
        }
        if (
            !password.any { it.isUpperCase() } ||
            !password.any { it.isLowerCase() } ||
            !password.any { it.isDigit() || !it.isLetterOrDigit() }
        ) {
            return false to "Your password is too weak, try making it more complex."
        }

        // Check if username is available using the passed jaSync instance
        val usernameExists = jaSync.executeQuery(
            "SELECT 1 FROM accounts WHERE username = '${username.replace("'", "''")}' LIMIT 1;"
        ).rows.isNotEmpty()
        if (usernameExists) {
            return false to "That username is already taken."
        }

        return true to ""
    }

    fun confirmRegistration(actor: Player, password: String) {
        val registration = pendingRegistrations.remove(actor)
        if (registration != null && registration.second == password) {
            val username = registration.first
            
            // Call Account.registerAccount to gather details and then insert into the database
            runBlocking {
                try {
                    val accountDetails = Account(jaSync, clerk.logger).registerAccount(actor, username, password)
                    val result = jaSync.insertNewAccount(accountDetails)
                    
                    actor.sendMessage(
                        Component.text(
                            "Registration successful! You have created a new account with the username '$username'.\nNow you can use /login with your credentials.",
                            NamedTextColor.GREEN
                        )
                    )
                    // Add green logger for registration
                    clerk.logger.info(Component.text("Player ${actor.username} registered a new account: $username", NamedTextColor.GREEN))
                } catch (e: Exception) {
                    clerk.logger.error("Registration error: ${e.message}")
                    e.printStackTrace()
                    actor.sendMessage(Component.text("Registration failed due to an error. Please try again later.", NamedTextColor.RED))
                    // Clear registration state in case of error
                    pendingRegistrations.remove(actor)
                    clerk.awaitingPasswordConfirmation.remove(actor)
                }
            }
        } else {
            // Clear any pending registration state for this player
            pendingRegistrations.remove(actor)
            clerk.awaitingPasswordConfirmation.remove(actor)
            actor.sendMessage(Component.text("Registration failed. Please try again.", NamedTextColor.RED))
        }
    }
}
