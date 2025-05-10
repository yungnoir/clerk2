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

class Login(private val clerk: Clerk, private val jaSync: JaSync) {

    @Command("login")
    fun attemptLogin(actor: Player, @Optional username: String?, @Optional password: String?) {
        if (clerk.unauthenticatedPlayers.contains(actor)) {
            if (username.isNullOrEmpty() || password.isNullOrEmpty()) {
                actor.sendMessage(Component.text("Usage: /login <username> <password>", NamedTextColor.YELLOW))
                return
            }
            val account = Account(jaSync, clerk.logger)
            val success = runBlocking {
                account.loginAccount(actor, username, password)
            }
            if (success) {
                clerk.unauthenticatedPlayers.remove(actor)
                // Log successful login in green
                clerk.logger.info(Component.text("Player ${actor.username} logged into account $username", NamedTextColor.GREEN))
                actor.disconnect(Component.text("You have successfully logged in.\nPlease reconnect to access the server."))
            }
            // No need for an else block as error messages are handled in loginAccount
        } else {
            actor.sendMessage(Component.text("You are already logged in.", NamedTextColor.RED))
        }
    }

    @Command("logout")
    fun logout(actor: Player) {
        runBlocking {
            Account(jaSync, clerk.logger).setLoggedOut(actor.username, true)
            clerk.unauthenticatedPlayers.add(actor)
            actor.disconnect(Component.text("You have been successfully logged out!", NamedTextColor.YELLOW))
        }
    }
}
