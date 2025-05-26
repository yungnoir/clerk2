package twizzy.tech.clerk.commands

import com.velocitypowered.api.proxy.Player
import net.kyori.adventure.text.Component
import net.kyori.adventure.text.format.NamedTextColor
import revxrsal.commands.annotation.Command
import revxrsal.commands.annotation.Cooldown
import twizzy.tech.clerk.Clerk
import twizzy.tech.clerk.util.JacksonFactory

class Lobby(private val clerk: Clerk) {

    private val langConfig = JacksonFactory.loadLangConfig()

    @Command("lobby", "hub")
    @Cooldown(10)
    fun lobby(actor: Player) {
        // Use the server instance directly from clerk
        val lobby = clerk.server.getServer("lobby")
        
        if (lobby.isPresent) {
            actor.createConnectionRequest(lobby.get()).fireAndForget()
            actor.sendMessage(Component.text(langConfig.getMessage("lobby.sending"), NamedTextColor.YELLOW))
        } else {
            actor.sendMessage(Component.text(langConfig.getMessage("lobby.not_found"), NamedTextColor.RED))
        }
    }
}
