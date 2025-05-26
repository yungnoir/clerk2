package twizzy.tech.clerk.commands

import com.velocitypowered.api.proxy.Player
import kotlinx.coroutines.runBlocking
import net.kyori.adventure.text.Component
import net.kyori.adventure.text.format.NamedTextColor
import net.kyori.adventure.text.format.TextDecoration
import revxrsal.commands.annotation.Command
import revxrsal.commands.annotation.Optional
import revxrsal.commands.annotation.Subcommand
import revxrsal.commands.velocity.annotation.CommandPermission
import twizzy.tech.clerk.Clerk
import twizzy.tech.clerk.player.Account
import twizzy.tech.clerk.util.JaSync
import twizzy.tech.clerk.util.JacksonFactory
import java.time.Instant
import java.util.regex.Pattern

@Command("permission", "permissions", "perm", "perms")
@CommandPermission("clerk.permission")
class Permission(private val clerk: Clerk) {
    private val jaSync = clerk.jaSync
    private val account = clerk.account

    private val langConfig = JacksonFactory.loadLangConfig()

    @Command("permission", "permissions", "perm", "perms")
    fun permissionUsage(actor: Player) {
        // Get the multi-line help message from the language configuration
        val helpMessage = langConfig.getMessage("permission.usage.help")

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

    /**
     * Adds a permission to a target user, optionally with a duration
     * Usage: /permission <target> add <permission> [duration]
     */
    @Subcommand("<target> add <permission> <duration>")
    @CommandPermission("clerk.permission")
    fun addPerm(
        actor: Player,
        @Account.CachedAccounts target: String?,
        @Optional permission: String?,
        @Optional duration: String?
    ) {
        if (target.isNullOrEmpty() || permission.isNullOrEmpty()) {
            actor.sendMessage(Component.text(langConfig.getMessage("permission.usage.add")))
            return
        }

        runBlocking {
            // Check if account exists
            val accountExists = jaSync.executeQuery(
                "SELECT 1 FROM accounts WHERE username = '${target.replace("'", "''")}'"
            ).rows.isNotEmpty()

            if (!accountExists) {
                actor.sendMessage(Component.text(langConfig.getMessage("permission.add.no_account", mapOf("target" to target))))
                return@runBlocking
            }

            // Handle regular vs. timed permissions
            if (duration.isNullOrBlank()) {
                // For permanent permissions
                val success = account.modifyPermission(target, permission, true, clerk.lettuce)
                if (success) {
                    actor.sendMessage(Component.text(langConfig.getMessage("permission.add.success", mapOf(
                        "permission" to permission,
                        "target" to target
                    ))))
                } else {
                    actor.sendMessage(Component.text(langConfig.getMessage("permission.add.failed", mapOf(
                        "permission" to permission,
                        "target" to target
                    ))))
                }
            } else {
                // For timed permissions
                val expireAt = parseDuration(duration)
                if (expireAt == null) {
                    actor.sendMessage(Component.text(langConfig.getMessage("permission.add.invalid_duration")))
                    return@runBlocking
                }

                // Create timed permission
                val timedPermission = mapOf(
                    "permission" to permission,
                    "expires" to expireAt.epochSecond
                )

                // Add timed permission
                val success = account.addTimedPermission(target, timedPermission, clerk.lettuce)
                if (success) {
                    actor.sendMessage(Component.text(langConfig.getMessage("permission.add.success_timed", mapOf(
                        "permission" to permission,
                        "target" to target,
                        "duration" to duration
                    ))))
                } else {
                    actor.sendMessage(Component.text(langConfig.getMessage("permission.add.failed", mapOf(
                        "permission" to permission,
                        "target" to target
                    ))))
                }
            }
        }
    }

    @Subcommand("<target> remove <permission>")
    @CommandPermission("clerk.permission")
    fun removePerm(
        actor: Player,
        @Account.CachedAccounts target: String?,
        @Optional permission: String?
    ) {
        if (target.isNullOrEmpty() || permission.isNullOrEmpty()) {
            actor.sendMessage(Component.text(langConfig.getMessage("permission.usage.remove")))
            return
        }

        runBlocking {
            // Check if account exists
            val accountExists = jaSync.executeQuery(
                "SELECT 1 FROM accounts WHERE username = '${target.replace("'", "''")}'"
            ).rows.isNotEmpty()

            if (!accountExists) {
                actor.sendMessage(Component.text(langConfig.getMessage("permission.remove.no_account", mapOf("target" to target))))
                return@runBlocking
            }

            // Remove permission (both regular and timed)
            val success = account.modifyPermission(target, permission, false, clerk.lettuce)

            if (success) {
                actor.sendMessage(Component.text(langConfig.getMessage("permission.remove.success", mapOf(
                    "permission" to permission,
                    "target" to target
                ))))
            } else {
                actor.sendMessage(Component.text(langConfig.getMessage("permission.remove.failed", mapOf(
                    "permission" to permission,
                    "target" to target
                ))))
            }
        }
    }

    @Subcommand("<target> list")
    @CommandPermission("clerk.permission")
    fun listPerms(
        actor: Player,
        @Account.CachedAccounts target: String?
    ) {
        if (target.isNullOrEmpty()) {
            actor.sendMessage(Component.text(langConfig.getMessage("permission.usage.list")))
            return
        }

        runBlocking {
            // Check if account exists
            val accountExists = jaSync.executeQuery(
                "SELECT 1 FROM accounts WHERE username = '${target.replace("'", "''")}'"
            ).rows.isNotEmpty()

            if (!accountExists) {
                actor.sendMessage(Component.text(langConfig.getMessage("permission.list.no_account", mapOf("target" to target))))
                return@runBlocking
            }

            // List permissions
            val permissions = account.listPermissions(target, clerk.lettuce)

            if (permissions.isEmpty()) {
                actor.sendMessage(Component.text(langConfig.getMessage("permission.list.none", mapOf("target" to target))))
            } else {
                val header = langConfig.getMessage("permission.list.header", mapOf("target" to target))
                val permsText = permissions.joinToString(separator = "\n") { "- $it" }
                actor.sendMessage(Component.text("$header\n$permsText"))
            }
        }
    }

    companion object {
        // Parses duration strings like "1d", "5h", "10m", "1w", "1mo", "1y", "1s" into an Instant in the future
        fun parseDuration(duration: String): Instant? {
            val pattern = Pattern.compile("(\\d+)(s|m|h|d|w|mo|y)", Pattern.CASE_INSENSITIVE)
            val matcher = pattern.matcher(duration)
            var totalSeconds = 0L
            var totalMonths = 0L
            var totalYears = 0L
            while (matcher.find()) {
                val value = matcher.group(1)?.toLongOrNull() ?: return null
                when (matcher.group(2)?.lowercase()) {
                    "s" -> totalSeconds += value
                    "m" -> totalSeconds += value * 60
                    "h" -> totalSeconds += value * 3600
                    "d" -> totalSeconds += value * 86400
                    "w" -> totalSeconds += value * 604800
                    "mo" -> totalMonths += value
                    "y" -> totalYears += value
                    else -> return null
                }
            }
            if (totalSeconds == 0L && totalMonths == 0L && totalYears == 0L) return null
            var instant = Instant.now().plusSeconds(totalSeconds)
            // For months and years, use OffsetDateTime for proper calendar math
            if (totalMonths > 0 || totalYears > 0) {
                val odt = java.time.OffsetDateTime.now()
                    .plusMonths(totalMonths)
                    .plusYears(totalYears)
                // Add the seconds offset
                instant = odt.toInstant().plusSeconds(totalSeconds)
            }
            return instant
        }
    }
}
