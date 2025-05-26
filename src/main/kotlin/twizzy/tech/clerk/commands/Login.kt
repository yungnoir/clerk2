package twizzy.tech.clerk.commands

import com.velocitypowered.api.proxy.Player
import kotlinx.coroutines.runBlocking
import net.kyori.adventure.text.Component
import net.kyori.adventure.text.format.NamedTextColor
import revxrsal.commands.annotation.Command
import revxrsal.commands.annotation.Optional
import twizzy.tech.clerk.Clerk
import twizzy.tech.clerk.player.Account
import twizzy.tech.clerk.util.JacksonFactory
import twizzy.tech.clerk.util.JaSync
import java.net.InetSocketAddress

class Login(private val clerk: Clerk) {

    private val account = clerk.account
    private val langConfig = JacksonFactory.loadLangConfig()

    @Command("login")
    fun attemptLogin(actor: Player, @Optional username: String?, @Optional password: String?) {
        if (clerk.unauthenticatedPlayers.contains(actor.uniqueId)) {
            if (username.isNullOrEmpty() || password.isNullOrEmpty()) {
                actor.sendMessage(Component.text(langConfig.getMessage("login.usage"), NamedTextColor.YELLOW))
                return
            }
            val success = runBlocking {
                account.loginAccount(actor, username, password)
            }
            if (success) {
                clerk.unauthenticatedPlayers.remove(actor.uniqueId)
                // Log successful login in green
                clerk.logger.info(Component.text("Player ${actor.username} logged into account $username", NamedTextColor.GREEN))
                actor.sendMessage(Component.text(langConfig.getMessage("login.successful"), NamedTextColor.GREEN))
                actor.transferToHost(InetSocketAddress("minemares.com", 25565))
            }
            // No need for an else block as error messages are handled in loginAccount
        } else {
            actor.sendMessage(Component.text(langConfig.getMessage("login.already_logged_in"), NamedTextColor.RED))
        }
    }

    @Command("logout")
    fun logout(actor: Player) {
        runBlocking {
            account.setLoggedOut(actor.username, true)
            actor.sendMessage(Component.text(langConfig.getMessage("login.logged_out"), NamedTextColor.YELLOW))
            actor.transferToHost(InetSocketAddress("minemares.com", 25565))
        }
    }
}
