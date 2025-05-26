package twizzy.tech.clerk.commands

import com.velocitypowered.api.proxy.Player
import net.kyori.adventure.text.Component
import net.kyori.adventure.text.format.NamedTextColor
import revxrsal.commands.annotation.Command
import revxrsal.commands.annotation.Subcommand
import revxrsal.commands.velocity.annotation.CommandPermission
import twizzy.tech.clerk.Clerk
import twizzy.tech.clerk.player.StaffManager
import twizzy.tech.clerk.util.JacksonFactory

@Command("staffchat", "sc")
@CommandPermission("clerk.staffchat")
class StaffChat(private val clerk: Clerk) {

    private val staffManager = clerk.staffManager

    private val langConfig = JacksonFactory.loadLangConfig()

    @Command("staffchat", "sc")
    fun toggleStaffChat(actor: Player) {
        // Toggle talking mode for staff chat
        val isTalking = staffManager.isTalking(actor)
        staffManager.setTalking(actor, !isTalking)

        if (staffManager.isTalking(actor)) {
            actor.sendMessage(Component.text(langConfig.getMessage("staff.chat_enabled"), NamedTextColor.GREEN))
        } else {
            actor.sendMessage(Component.text(langConfig.getMessage("staff.chat_disabled"), NamedTextColor.RED))
        }
    }

    @Subcommand("hide")
    fun hideStaffChat(actor: Player) {
        // Toggle listening status
        val isCurrentlyListening = staffManager.isListening(actor)
        staffManager.setListening(actor, !isCurrentlyListening)

        // If turning off listening, also turn off talking
        if (!isCurrentlyListening) {
            actor.sendMessage(Component.text(langConfig.getMessage("staff.listening_enabled"), NamedTextColor.GREEN))
        } else {
            staffManager.setTalking(actor, false) // Turn off talking when hiding
            actor.sendMessage(Component.text(langConfig.getMessage("staff.listening_disabled"), NamedTextColor.RED))
        }
    }

    @Command("staffchat <message>", "sc <message>")
    fun sendStaffMessage(actor: Player, message: String) {
        // Ensure the player is listening when they send a direct message
        if (!staffManager.isListening(actor)) {
            staffManager.setListening(actor, true)
            actor.sendMessage(Component.text(langConfig.getMessage("staff.listening_enabled"), NamedTextColor.GREEN))
        }

        // Send the message
        staffManager.sendStaffMessage("${actor.username}: $message")
    }
}

