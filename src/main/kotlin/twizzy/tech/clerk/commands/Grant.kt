package twizzy.tech.clerk.commands

import com.velocitypowered.api.proxy.Player
import kotlinx.coroutines.launch
import net.kyori.adventure.text.Component
import net.kyori.adventure.text.format.NamedTextColor
import net.kyori.adventure.text.format.TextDecoration
import revxrsal.commands.annotation.Command
import revxrsal.commands.annotation.Optional
import revxrsal.commands.annotation.Subcommand
import revxrsal.commands.velocity.annotation.CommandPermission
import twizzy.tech.clerk.Clerk
import twizzy.tech.clerk.player.Account
import twizzy.tech.clerk.player.Ranks
import twizzy.tech.clerk.util.JacksonFactory
import java.time.Instant
import java.util.regex.Pattern


@Command("grant")
@CommandPermission("clerk.grant")
class Grant(private val clerk: Clerk) {

    private val ranks = clerk.ranks
    private val langConfig = JacksonFactory.loadLangConfig()

    @Command("grant")
    fun grantUsage(actor: Player) {
        // Get the multi-line help message from the language configuration
        val helpMessage = langConfig.getMessage("grant.usage.help")

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

    @Subcommand("<target> <rank>")
    @CommandPermission("clerk.grant.add")
    fun grantRank(
        actor: Player,
        @Optional @Account.CachedAccounts target: String?,
        @Optional @Ranks.CachedRanks rank: String?,
        @Optional duration: String?
    ){
        if (target.isNullOrEmpty() || rank.isNullOrEmpty()) {
            actor.sendMessage(Component.text(langConfig.getMessage("grant.add.usage")))
            return
        }

        // Validate duration if provided
        if (duration != null) {
            val expireAt = parseDuration(duration)
            if (expireAt == null) {
                actor.sendMessage(Component.text(langConfig.getMessage("grant.add.invalid_duration")))
                return
            }
        }

        // Use MCCoroutine scope instead of runBlocking
        clerk.scope.launch {
            // Check if the rank exists
            val rankObj = ranks.getRank(rank)
            if (rankObj == null) {
                actor.sendMessage(Component.text(langConfig.getMessage("grant.add.rank_not_found", mapOf("rank" to rank))))
                return@launch
            }
            
            // Grant the rank to the target with optional duration
            val success = ranks.grantRank(target, rank, duration)
            
            if (success) {
                if (duration != null) {
                    actor.sendMessage(Component.text(langConfig.getMessage("grant.add.success_temp", mapOf(
                        "rank" to rank,
                        "target" to target,
                        "duration" to duration
                    ))))
                } else {
                    actor.sendMessage(Component.text(langConfig.getMessage("grant.add.success_perm", mapOf(
                        "rank" to rank,
                        "target" to target
                    ))))
                }
            } else {
                actor.sendMessage(Component.text(langConfig.getMessage("grant.add.failed", mapOf(
                    "rank" to rank,
                    "target" to target
                ))))
            }
        }
    }
    
    @Subcommand("<target> remove <rank>")
    @CommandPermission("clerk.grant.remove")
    fun removeRank(
        actor: Player,
        @Account.CachedAccounts target: String?,
        @Optional @Ranks.CachedRanks rank: String?
    ){
        if (target.isNullOrEmpty() || rank.isNullOrEmpty()) {
            actor.sendMessage(Component.text(langConfig.getMessage("grant.remove.usage")))
            return
        }
        
        // Use MCCoroutine scope instead of runBlocking
        clerk.scope.launch {
            // Check if the rank exists
            val rankObj = ranks.getRank(rank)
            if (rankObj == null) {
                actor.sendMessage(Component.text(langConfig.getMessage("grant.remove.rank_not_found", mapOf("rank" to rank))))
                return@launch
            }
            
            // Remove the rank from the target
            val success = ranks.removeRank(target, rank)
            
            if (success) {
                actor.sendMessage(Component.text(langConfig.getMessage("grant.remove.success", mapOf(
                    "rank" to rank,
                    "target" to target
                ))))
            } else {
                actor.sendMessage(Component.text(langConfig.getMessage("grant.remove.failed", mapOf(
                    "rank" to rank,
                    "target" to target
                ))))
            }
        }
    }

    @Command("grants <target>")
    @CommandPermission("clerk.grant.view")
    fun listGrants(
        actor: Player,
        @Optional @Account.CachedAccounts target: String?
    ) {
        if (target.isNullOrEmpty()) {
            actor.sendMessage(Component.text(langConfig.getMessage("grants.list.usage")))
            return
        }
        
        // Use MCCoroutine scope instead of runBlocking
        clerk.scope.launch {
            // Use the new getUserRanks method to get ranks with expiration info
            val userRanks = ranks.getUserRanks(target)
            
            if (userRanks.isEmpty()) {
                actor.sendMessage(Component.text(langConfig.getMessage("grants.list.none", mapOf("target" to target))))
                return@launch
            }

            // Display ranks to the actor
            actor.sendMessage(Component.text(langConfig.getMessage("grants.list.header", mapOf("target" to target))))

            userRanks.forEachIndexed { index, userRank ->
                // Build the rank display with expiration info if present
                val builder = Component.text()
                    .append(Component.text(langConfig.getMessage("grants.list.entry", mapOf(
                        "index" to (index + 1).toString(),
                        "rank" to userRank.rank
                    ))))

                // Add expiration info if this rank has an expiration
                userRank.getExpirationTime()?.let { expirationTime ->
                    val timeLeft = ranks.formatTimeUntilExpiration(expirationTime)
                    builder.append(Component.text(langConfig.getMessage("grants.list.expiry", mapOf("time" to timeLeft))))
                } ?: builder.append(Component.text(langConfig.getMessage("grants.list.permanent")))

                actor.sendMessage(builder.build())
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
