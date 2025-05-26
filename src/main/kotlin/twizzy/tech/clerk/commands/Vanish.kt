package twizzy.tech.clerk.commands

import com.velocitypowered.api.proxy.Player
import kotlinx.coroutines.launch
import net.kyori.adventure.text.Component
import net.kyori.adventure.text.format.NamedTextColor
import revxrsal.commands.annotation.Command
import twizzy.tech.clerk.Clerk
import twizzy.tech.clerk.player.Account
import twizzy.tech.clerk.player.StaffManager
import twizzy.tech.clerk.util.JacksonFactory
import twizzy.tech.clerk.util.Lettuce

@Command("vanish")
class Vanish(private val clerk: Clerk) {

    private val staffManager = clerk.staffManager
    // Use the shared Account instance from Clerk instead of creating a new one
    private val account = clerk.account
    private val langConfig = JacksonFactory.loadLangConfig()

    @Command("vanish", "v")
    fun toggleVanish(actor: Player) {
        val isVanished = staffManager.vanishToggle(actor)

        // Send feedback to the player based on their new vanish state
        if (isVanished) {
            actor.sendMessage(Component.text(langConfig.getMessage("vanish.now_vanished")))
        } else {
            actor.sendMessage(Component.text(langConfig.getMessage("vanish.now_visible")))
        }
    }

    @Command("vanish auto")
    suspend fun autoVanish(actor: Player) {
        val newSetting = account.toggleSetting(actor.username, "autoVanish", clerk.lettuce)
        // Display message based on the new setting value
        if (newSetting == true) {
            actor.sendMessage(Component.text(langConfig.getMessage("vanish.auto_enabled")))
        } else {
            actor.sendMessage(Component.text(langConfig.getMessage("vanish.auto_disabled")))
        }
    }
}

