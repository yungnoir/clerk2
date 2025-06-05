package twizzy.tech.clerk.commands

import com.velocitypowered.api.proxy.Player
import kotlinx.coroutines.launch
import net.kyori.adventure.text.Component
import net.kyori.adventure.text.format.NamedTextColor
import net.kyori.adventure.text.format.TextDecoration
import revxrsal.commands.annotation.Command
import revxrsal.commands.annotation.Optional
import revxrsal.commands.annotation.Subcommand
import revxrsal.commands.annotation.Suggest
import revxrsal.commands.velocity.annotation.CommandPermission
import twizzy.tech.clerk.Clerk
import twizzy.tech.clerk.player.Ranks
import twizzy.tech.clerk.util.JaSync
import twizzy.tech.clerk.util.JacksonFactory

@Command("rank", "ranks")
@CommandPermission("clerk.rank")
class Rank(private val clerk: Clerk) {

    private val ranks = clerk.ranks

    private val langConfig = JacksonFactory.loadLangConfig()

    @Command("rank", "ranks")
    fun rankUsage(actor: Player) {
        // Get the multi-line help message from the language configuration
        val helpMessage = langConfig.getMessage("rank.usage.help")

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
    
    @Subcommand("create <name>")
    fun createRank(
        actor: Player,
        @Optional name: String?
    ) {
        val name = name?.split(" ")?.firstOrNull()
        if (name.isNullOrEmpty()) {
            actor.sendMessage(Component.text(langConfig.getMessage("rank.create.usage")))
            return
        }

        // Use MCCoroutine scope instead of blocking
        clerk.scope.launch {
            // Check if the rank already exists
            val existingRank = ranks.getRank(name)
            if (existingRank != null) {
                actor.sendMessage(Component.text(langConfig.getMessage("rank.create.exists", mapOf("name" to name))))
                return@launch
            }
            // Create rank with default values
            val success = ranks.setRank(
                name = name,
                prefix = "§7", // Default gray color
                permissions = emptyList(),
                inheritance = emptyList(),
                weight = 0     // Default weight
            )
            if (success) {
                actor.sendMessage(Component.text(langConfig.getMessage("rank.create.success", mapOf("name" to name))))
            } else {
                actor.sendMessage(Component.text(langConfig.getMessage("rank.create.failed", mapOf("name" to name))))
            }
        }
    }

    @Subcommand("delete <name>")
    fun deleteRank(
        actor: Player,
        @Optional @Ranks.CachedRanks name: String?
    ) {
        val name = name?.split(" ")?.firstOrNull()
        if (name.isNullOrEmpty()) {
            actor.sendMessage(Component.text(langConfig.getMessage("rank.delete.usage")))
            return
        }

        // Use MCCoroutine scope instead of blocking
        clerk.scope.launch {
            // Check if the rank exists
            val existingRank = ranks.getRank(name)
            if (existingRank == null) {
                actor.sendMessage(Component.text(langConfig.getMessage("rank.delete.not_found", mapOf("name" to name))))
                return@launch
            }
            val success = ranks.deleteRank(name)
            if (success) {
                actor.sendMessage(Component.text(langConfig.getMessage("rank.delete.success", mapOf("name" to name))))
            } else {
                actor.sendMessage(Component.text(langConfig.getMessage("rank.delete.failed", mapOf("name" to name))))
            }
        }
    }

    @Subcommand("list")
    fun listRanks(
        actor: Player
    ) {
        val allRanks = ranks.getAllRanks()

        if (allRanks.isEmpty()) {
            actor.sendMessage(Component.text(langConfig.getMessage("rank.list.none")))
            return
        }

        actor.sendMessage(Component.text(langConfig.getMessage("rank.list.header")))

        // Sort ranks by weight (highest first)
        val sortedRanks = allRanks.values.sortedByDescending { it.weight }

        for (rank in sortedRanks) {
            val defaultTag = if (rank.isDefault) langConfig.getMessage("rank.info.default_tag") else ""
            val prefix = langConfig.getMessage("rank.info.prefix", mapOf("prefix" to rank.prefix))
            val weight = langConfig.getMessage("rank.info.weight", mapOf("weight" to rank.weight.toString()))

            actor.sendMessage(Component.text()
                .append(Component.text("${rank.name}$defaultTag: ", NamedTextColor.YELLOW))
                .append(Component.text("$weight, ", NamedTextColor.WHITE))
                .append(Component.text(prefix, NamedTextColor.WHITE))
                .build()
            )
        }
    }
    
    @Subcommand("info <name>")
    fun rankInfo(
        actor: Player,
        @Optional @Ranks.CachedRanks name: String?
    ) {
        val name = name?.split(" ")?.firstOrNull()
        if (name.isNullOrEmpty()) {
            actor.sendMessage(Component.text(langConfig.getMessage("rank.info.usage")))
            return
        }

        val rank = ranks.getRank(name)

        if (rank == null) {
            actor.sendMessage(Component.text(langConfig.getMessage("rank.info.not_found", mapOf("name" to name))))
            return
        }

        val defaultTag = if (rank.isDefault) langConfig.getMessage("rank.info.default_tag") else ""
        actor.sendMessage(Component.text(langConfig.getMessage("rank.info.header", mapOf(
            "name" to name,
            "default" to defaultTag
        ))))

        actor.sendMessage(Component.text(langConfig.getMessage("rank.info.prefix", mapOf("prefix" to rank.prefix))))
        actor.sendMessage(Component.text(langConfig.getMessage("rank.info.weight", mapOf("weight" to rank.weight.toString()))))

        actor.sendMessage(Component.text(langConfig.getMessage("rank.info.permissions_header")))
        if (rank.permissions.isEmpty()) {
            actor.sendMessage(Component.text(langConfig.getMessage("rank.info.permissions_none")))
        } else {
            rank.permissions.forEach { perm ->
                actor.sendMessage(Component.text(langConfig.getMessage("rank.info.permissions_entry", mapOf("permission" to perm))))
            }
        }

        actor.sendMessage(Component.text(langConfig.getMessage("rank.info.inheritance_header")))
        if (rank.inheritance.isEmpty()) {
            actor.sendMessage(Component.text(langConfig.getMessage("rank.info.inheritance_none")))
        } else {
            rank.inheritance.forEach { parent ->
                actor.sendMessage(Component.text(langConfig.getMessage("rank.info.inheritance_entry", mapOf("parent" to parent))))
            }
        }

        actor.sendMessage(Component.text(langConfig.getMessage("rank.info.users_header", mapOf("count" to rank.users.size.toString()))))
        if (rank.users.isNotEmpty()) {
            val userList = rank.users.joinToString(", ")
            actor.sendMessage(Component.text(langConfig.getMessage("rank.info.users_list", mapOf("users" to userList))))
        }
    }

    @Subcommand("setprefix <name> <prefix>")
    fun setPrefix(
        actor: Player,
        @Optional @Ranks.CachedRanks name: String?,
        @Optional @Suggest("§") prefix: String?
    ) {
        if (name.isNullOrEmpty() || prefix.isNullOrEmpty()) {
            actor.sendMessage(Component.text(langConfig.getMessage("rank.setprefix.usage")))
            return
        }
        val prefix = prefix.replace('&', '§')

        // Use MCCoroutine scope instead of blocking
        clerk.scope.launch {
            val rank = ranks.getRank(name)

            if (rank == null) {
                actor.sendMessage(Component.text(langConfig.getMessage("rank.setprefix.not_found", mapOf("name" to name))))
                return@launch
            }

            val success = ranks.setRank(
                name = rank.name,
                prefix = prefix,
                permissions = rank.permissions,
                inheritance = rank.inheritance,
                weight = rank.weight,
                users = rank.users,
                isDefault = rank.isDefault
            )

            if (success) {
                actor.sendMessage(Component.text(langConfig.getMessage("rank.setprefix.success", mapOf(
                    "name" to name,
                    "prefix" to prefix
                ))))
            } else {
                actor.sendMessage(Component.text(langConfig.getMessage("rank.setprefix.failed", mapOf("name" to name))))
            }
        }
    }

    @Subcommand("setweight <name> <weight>")
    fun setWeight(
        actor: Player,
        @Optional @Ranks.CachedRanks name: String?,
        @Optional weight: Int?
    ) {
        if (name.isNullOrEmpty()) {
            actor.sendMessage(Component.text(langConfig.getMessage("rank.setweight.usage")))
            return
        }

        // Use MCCoroutine scope instead of blocking
        clerk.scope.launch {
            val rank = ranks.getRank(name)

            if (rank == null) {
                actor.sendMessage(Component.text(langConfig.getMessage("rank.setweight.not_found", mapOf("name" to name))))
                return@launch
            }

            val success = ranks.setRank(
                name = rank.name,
                prefix = rank.prefix,
                permissions = rank.permissions,
                inheritance = rank.inheritance,
                weight = weight,
                users = rank.users,
                isDefault = rank.isDefault
            )

            if (success) {
                actor.sendMessage(Component.text(langConfig.getMessage("rank.setweight.success", mapOf(
                    "name" to name,
                    "weight" to weight.toString()
                ))))
            } else {
                actor.sendMessage(Component.text(langConfig.getMessage("rank.setweight.failed", mapOf("name" to name))))
            }
        }
    }

    @Subcommand("permission add <name> <permission>")
    fun addPermission(
        actor: Player,
        @Optional @Ranks.CachedRanks name: String?,
        @Optional permission: String?
    ) {
        if (name.isNullOrEmpty() || permission.isNullOrEmpty()) {
            actor.sendMessage(Component.text(langConfig.getMessage("rank.permission.add.usage")))
            return
        }

        // Use MCCoroutine scope instead of blocking
        clerk.scope.launch {
            val rank = ranks.getRank(name)

            if (rank == null) {
                actor.sendMessage(Component.text(langConfig.getMessage("rank.permission.add.not_found", mapOf("name" to name))))
                return@launch
            }

            val updatedPerms = if (permission !in rank.permissions) {
                rank.permissions + permission
            } else {
                actor.sendMessage(Component.text(langConfig.getMessage("rank.permission.add.exists", mapOf(
                    "permission" to permission,
                    "name" to name
                ))))
                return@launch
            }

            val success = ranks.setRank(
                name = rank.name,
                prefix = rank.prefix,
                permissions = updatedPerms,
                inheritance = rank.inheritance,
                weight = rank.weight,
                users = rank.users,
                isDefault = rank.isDefault
            )

            if (success) {
                actor.sendMessage(Component.text(langConfig.getMessage("rank.permission.add.success", mapOf(
                    "permission" to permission,
                    "name" to name
                ))))
            } else {
                actor.sendMessage(Component.text(langConfig.getMessage("rank.permission.add.failed", mapOf("name" to name))))
            }
        }
    }

    @Subcommand("permission remove <name> <permission>")
    fun removePermission(
        actor: Player,
        @Optional @Ranks.CachedRanks name: String?,
        @Optional permission: String?
    ) {
        if (name.isNullOrEmpty() || permission.isNullOrEmpty()) {
            actor.sendMessage(Component.text(langConfig.getMessage("rank.permission.remove.usage")))
            return
        }

        // Use MCCoroutine scope instead of blocking
        clerk.scope.launch {
            val rank = ranks.getRank(name)

            if (rank == null) {
                actor.sendMessage(Component.text(langConfig.getMessage("rank.permission.remove.not_found", mapOf("name" to name))))
                return@launch
            }

            val updatedPerms = if (permission in rank.permissions) {
                rank.permissions.filter { it != permission }
            } else {
                actor.sendMessage(Component.text(langConfig.getMessage("rank.permission.remove.not_exists", mapOf(
                    "permission" to permission,
                    "name" to name
                ))))
                return@launch
            }

            val success = ranks.setRank(
                name = rank.name,
                prefix = rank.prefix,
                permissions = updatedPerms,
                inheritance = rank.inheritance,
                weight = rank.weight,
                users = rank.users,
                isDefault = rank.isDefault
            )

            if (success) {
                actor.sendMessage(Component.text(langConfig.getMessage("rank.permission.remove.success", mapOf(
                    "permission" to permission,
                    "name" to name
                ))))
            } else {
                actor.sendMessage(Component.text(langConfig.getMessage("rank.permission.remove.failed", mapOf("name" to name))))
            }
        }
    }

    @Subcommand("inherit add <name> <parent>")
    fun addInheritance(
        actor: Player,
        @Optional @Ranks.CachedRanks name: String?,
        @Optional @Ranks.CachedRanks parent: String?
    ) {
        if (name.isNullOrEmpty() || parent.isNullOrEmpty()) {
            actor.sendMessage(Component.text(langConfig.getMessage("rank.inherit.add.usage")))
            return
        }

        // Use MCCoroutine scope instead of blocking
        clerk.scope.launch {
            val rank = ranks.getRank(name)
            val parentRank = ranks.getRank(parent)

            if (rank == null) {
                actor.sendMessage(Component.text(langConfig.getMessage("rank.inherit.add.not_found", mapOf("name" to name))))
                return@launch
            }

            if (parentRank == null) {
                actor.sendMessage(Component.text(langConfig.getMessage("rank.inherit.add.parent_not_found", mapOf("parent" to parent))))
                return@launch
            }

            // Prevent circular inheritance
            if (name == parent) {
                actor.sendMessage(Component.text(langConfig.getMessage("rank.inherit.add.circular")))
                return@launch
            }

            val updatedInheritance = if (parent !in rank.inheritance) {
                rank.inheritance + parent
            } else {
                actor.sendMessage(Component.text(langConfig.getMessage("rank.inherit.add.exists", mapOf(
                    "name" to name,
                    "parent" to parent
                ))))
                return@launch
            }

            val success = ranks.setRank(
                name = rank.name,
                prefix = rank.prefix,
                permissions = rank.permissions,
                inheritance = updatedInheritance,
                weight = rank.weight,
                users = rank.users,
                isDefault = rank.isDefault
            )

            if (success) {
                actor.sendMessage(Component.text(langConfig.getMessage("rank.inherit.add.success", mapOf(
                    "name" to name,
                    "parent" to parent
                ))))
            } else {
                actor.sendMessage(Component.text(langConfig.getMessage("rank.inherit.add.failed", mapOf("name" to name))))
            }
        }
    }

    @Subcommand("inherit remove <name> <parent>")
    fun removeInheritance(
        actor: Player,
        @Optional @Ranks.CachedRanks name: String?,
        @Optional @Ranks.CachedRanks parent: String?
    ) {
        if (name.isNullOrEmpty() || parent.isNullOrEmpty()) {
            actor.sendMessage(Component.text(langConfig.getMessage("rank.inherit.remove.usage")))
            return
        }

        // Use MCCoroutine scope instead of blocking
        clerk.scope.launch {
            val rank = ranks.getRank(name)

            if (rank == null) {
                actor.sendMessage(Component.text(langConfig.getMessage("rank.inherit.remove.not_found", mapOf("name" to name))))
                return@launch
            }

            val updatedInheritance = if (parent in rank.inheritance) {
                rank.inheritance.filter { it != parent }
            } else {
                actor.sendMessage(Component.text(langConfig.getMessage("rank.inherit.remove.not_exists", mapOf(
                    "name" to name,
                    "parent" to parent
                ))))
                return@launch
            }

            val success = ranks.setRank(
                name = rank.name,
                prefix = rank.prefix,
                permissions = rank.permissions,
                inheritance = updatedInheritance,
                weight = rank.weight,
                users = rank.users,
                isDefault = rank.isDefault
            )

            if (success) {
                actor.sendMessage(Component.text(langConfig.getMessage("rank.inherit.remove.success", mapOf(
                    "name" to name,
                    "parent" to parent
                ))))
            } else {
                actor.sendMessage(Component.text(langConfig.getMessage("rank.inherit.remove.failed", mapOf("name" to name))))
            }
        }
    }

    @Command("ranks save")
    fun saveRanks(
        actor: Player
    ) {
        // Use MCCoroutine scope instead of blocking
        clerk.scope.launch {
            val success = ranks.saveRanks()

            if (success) {
                actor.sendMessage(Component.text(langConfig.getMessage("rank.save.success")))
            } else {
                actor.sendMessage(Component.text(langConfig.getMessage("rank.save.failed")))
            }
        }
    }

    @Command("ranks load")
    fun loadRanks(
        actor: Player
    ) {
        // Use MCCoroutine scope instead of blocking
        clerk.scope.launch {
            val success = ranks.loadRanks()

            if (success) {
                actor.sendMessage(Component.text(langConfig.getMessage("rank.load.success")))
            } else {
                actor.sendMessage(Component.text(langConfig.getMessage("rank.load.failed")))
            }
        }
    }
}
