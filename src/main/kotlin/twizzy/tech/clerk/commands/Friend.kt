package twizzy.tech.clerk.commands

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.velocitypowered.api.proxy.Player
import kotlinx.coroutines.*
import net.kyori.adventure.text.Component
import net.kyori.adventure.text.format.NamedTextColor
import net.kyori.adventure.text.format.TextDecoration
import revxrsal.commands.annotation.Command
import revxrsal.commands.annotation.Cooldown
import revxrsal.commands.annotation.Optional
import revxrsal.commands.annotation.Subcommand
import twizzy.tech.clerk.Clerk
import twizzy.tech.clerk.player.Account
import twizzy.tech.clerk.player.ConnectionHandler
import twizzy.tech.clerk.player.Friends
import twizzy.tech.clerk.util.JacksonFactory
import twizzy.tech.clerk.util.JaSync

@Command("friend")
class Friend(private val clerk: Clerk, ) {
    // Use centralized Account instance
    private val account = clerk.account
    private val friends = clerk.friends
    private val langConfig = JacksonFactory.loadLangConfig()


    @Command("friend")
    fun friendUsage(actor: Player) {
        // Get the multi-line help message from the language configuration
        val helpMessage = langConfig.getMessage("friend.usage.help")

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

    @Subcommand("add <target>")
    @Cooldown(5)
    fun addFriend(
        actor: Player,
        @Optional @ConnectionHandler.OnlinePlayers target: String?
    ) {
        if (target.isNullOrEmpty()) {
            actor.sendMessage(Component.text(langConfig.getMessage("friend.usage.add"), NamedTextColor.WHITE))
            return
        }

        // Check if player is trying to add themselves
        if (target.equals(actor.username, ignoreCase = true)) {
            actor.sendMessage(Component.text(langConfig.getMessage("friend.add.self"), NamedTextColor.YELLOW))
            return
        }

        clerk.scope.launch {
            val result = friends.addFriend(actor.username, target)

            when (result) {
                Friends.FriendActionResult.REQUEST_SENT -> {
                    // Normal request sent
                    actor.sendMessage(Component.text(langConfig.getMessage("friend.add.request_sent", mapOf("target" to target)), NamedTextColor.GREEN))

                    // Notify the target player if they are online
                    clerk.server.getPlayer(target).ifPresent { targetPlayer ->
                        if (targetPlayer.currentServer.isPresent &&
                            targetPlayer.currentServer.get().serverInfo.name != "auth") {

                            val requestNotification = Component.text(langConfig.getMessage("friend.add.incoming_request", mapOf("player" to actor.username)) + "\n        ", NamedTextColor.GREEN)
                                .append(
                                    Component.text(langConfig.getMessage("friend.requests.accept_button"), NamedTextColor.GREEN, TextDecoration.BOLD)
                                        .clickEvent(net.kyori.adventure.text.event.ClickEvent.runCommand("/friend add ${actor.username}"))
                                        .hoverEvent(Component.text(langConfig.getMessage("friend.add.accept_hover")))
                                )
                                .append(Component.text("    "))
                                .append(
                                    Component.text(langConfig.getMessage("friend.requests.deny_button"), NamedTextColor.RED, TextDecoration.BOLD)
                                        .clickEvent(net.kyori.adventure.text.event.ClickEvent.runCommand("/friend deny ${actor.username}"))
                                        .hoverEvent(Component.text(langConfig.getMessage("friend.add.deny_hover")))
                                )

                            targetPlayer.sendMessage(requestNotification)
                        }
                    }
                }

                Friends.FriendActionResult.NOW_FRIENDS -> {
                    // This was a mutual request that got auto-accepted
                    actor.sendMessage(Component.text(langConfig.getMessage("friend.add.now_friends", mapOf("target" to target)), NamedTextColor.GREEN))

                    // Notify the target player if they are online
                    clerk.server.getPlayer(target).ifPresent { targetPlayer ->
                        if (targetPlayer.currentServer.isPresent &&
                            targetPlayer.currentServer.get().serverInfo.name != "auth") {

                            val acceptNotification = Component.text(langConfig.getMessage("friend.add.now_friends", mapOf("target" to actor.username)), NamedTextColor.GREEN)
                            targetPlayer.sendMessage(acceptNotification)
                        }
                    }
                }

                Friends.FriendActionResult.ALREADY_FRIENDS -> {
                    actor.sendMessage(Component.text(langConfig.getMessage("friend.add.already_friends", mapOf("target" to target)), NamedTextColor.YELLOW))
                }

                Friends.FriendActionResult.REQUEST_ALREADY_SENT -> {
                    actor.sendMessage(Component.text(langConfig.getMessage("friend.add.request_already_sent", mapOf("target" to target)), NamedTextColor.YELLOW))
                }

                Friends.FriendActionResult.USER_NOT_FOUND -> {
                    actor.sendMessage(Component.text(langConfig.getMessage("friend.add.user_not_found", mapOf("target" to target)), NamedTextColor.RED))
                }

                Friends.FriendActionResult.REQUESTS_DISABLED -> {
                    actor.sendMessage(Component.text(langConfig.getMessage("friend.add.requests_disabled", mapOf("target" to target)), NamedTextColor.RED))
                }

                Friends.FriendActionResult.SELF_REQUEST -> {
                    actor.sendMessage(Component.text(langConfig.getMessage("friend.add.self"), NamedTextColor.YELLOW))
                }

                else -> {
                    actor.sendMessage(Component.text(langConfig.getMessage("friend.add.error"), NamedTextColor.RED))
                }
            }
        }
    }

    @Subcommand("remove")
    @Cooldown(5)
    fun removeFriend(
        actor: Player,
        @Optional @Friends.FriendList target: String?
    ) {
        if (target.isNullOrEmpty()) {
            actor.sendMessage(Component.text(langConfig.getMessage("friend.usage.remove"), NamedTextColor.WHITE))
            return
        }

        clerk.scope.launch {
            val result = friends.removeFriend(actor.username, target)

            when (result) {
                Friends.FriendActionResult.FRIEND_REMOVED -> {
                    actor.sendMessage(Component.text(langConfig.getMessage("friend.remove.removed", mapOf("target" to target)), NamedTextColor.YELLOW))
                }

                Friends.FriendActionResult.REQUEST_CANCELLED -> {
                    actor.sendMessage(Component.text(langConfig.getMessage("friend.remove.request_cancelled", mapOf("target" to target)), NamedTextColor.YELLOW))
                }

                Friends.FriendActionResult.NOT_FRIENDS -> {
                    actor.sendMessage(Component.text(langConfig.getMessage("friend.remove.not_friends", mapOf("target" to target)), NamedTextColor.RED))
                }

                else -> {
                    actor.sendMessage(Component.text(langConfig.getMessage("friend.remove.error", mapOf("target" to target)), NamedTextColor.RED))
                }
            }
        }
    }

    @Subcommand("deny")
    @Cooldown(5)
    fun denyFriendRequest(
        actor: Player,
        @Optional @Friends.FriendRequests target: String?
    ) {
        if (target.isNullOrEmpty()) {
            actor.sendMessage(Component.text(langConfig.getMessage("friend.usage.deny"), NamedTextColor.WHITE))
            return
        }

        clerk.scope.launch {
            val success = friends.denyFriend(target, actor.username)
            if (success) {
                actor.sendMessage(Component.text(langConfig.getMessage("friend.deny.success", mapOf("target" to target)), NamedTextColor.YELLOW))
            } else {
                actor.sendMessage(Component.text(langConfig.getMessage("friend.deny.no_request", mapOf("target" to target)), NamedTextColor.RED))
            }
        }
    }

    @Subcommand("requests")
    @Cooldown(10)
    fun listRequests(
        actor: Player
    ) {
        clerk.scope.launch {
            val incoming = friends.getRequests(actor.username)
            val outgoing = friends.getOutgoingRequests(actor.username)

            actor.sendMessage(Component.text("                                                                              ", NamedTextColor.DARK_GREEN, TextDecoration.STRIKETHROUGH))
            actor.sendMessage(Component.text(langConfig.getMessage("friend.requests.header"), NamedTextColor.DARK_GREEN))

            if (incoming.isEmpty() && outgoing.isEmpty()) {
                actor.sendMessage(Component.text(langConfig.getMessage("friend.requests.none"), NamedTextColor.GRAY))
                return@launch
            }

            if (incoming.isNotEmpty()) {
                actor.sendMessage(Component.text(langConfig.getMessage("friend.requests.incoming_header"), NamedTextColor.GREEN))
                incoming.forEach { username ->
                    actor.sendMessage(
                        Component.text(langConfig.getMessage("friend.requests.incoming_format", mapOf("username" to username)), NamedTextColor.GRAY)
                            .append(
                                Component.text(langConfig.getMessage("friend.requests.accept_button"), NamedTextColor.GREEN, TextDecoration.BOLD)
                                    .hoverEvent(Component.text(langConfig.getMessage("friend.requests.accept_hover")))
                                    .clickEvent(net.kyori.adventure.text.event.ClickEvent.runCommand("/friend add $username"))
                            )
                            .append(Component.text(" ", NamedTextColor.GRAY))
                            .append(
                                Component.text(langConfig.getMessage("friend.requests.deny_button"), NamedTextColor.RED, TextDecoration.BOLD)
                                    .hoverEvent(Component.text(langConfig.getMessage("friend.requests.deny_hover")))
                                    .clickEvent(net.kyori.adventure.text.event.ClickEvent.runCommand("/friend deny $username"))
                            )
                    )
                }
            }

            if (outgoing.isNotEmpty()) {
                actor.sendMessage(Component.text(langConfig.getMessage("friend.requests.outgoing_header"), NamedTextColor.GREEN))
                outgoing.forEach { username ->
                    actor.sendMessage(
                        Component.text(langConfig.getMessage("friend.requests.outgoing_format", mapOf("username" to username)), NamedTextColor.GRAY)
                            .append(
                                Component.text(langConfig.getMessage("friend.requests.cancel_button"), NamedTextColor.RED)
                                    .hoverEvent(Component.text(langConfig.getMessage("friend.requests.cancel_hover")))
                                    .clickEvent(net.kyori.adventure.text.event.ClickEvent.runCommand("/friend remove $username"))
                            )
                    )
                }
            }

            actor.sendMessage(Component.text(langConfig.getMessage("friend.requests.toggle_info"), NamedTextColor.GRAY, TextDecoration.ITALIC))
        }
    }

    @Subcommand("requests toggle")
    @Cooldown(5)
    fun toggleRequests(
        actor: Player
    ) {
        clerk.scope.launch {
            // Use the toggleSetting method which handles getting and toggling in one operation
            val newSetting = account.toggleSetting(actor.username, "toggledRequests", clerk.lettuce)
            // Display message based on the new setting value
            if (newSetting == true) {
                actor.sendMessage(Component.text(langConfig.getMessage("friend.requests.toggle_off"), NamedTextColor.YELLOW))
            } else {
                actor.sendMessage(Component.text(langConfig.getMessage("friend.requests.toggle_on"), NamedTextColor.GREEN))
            }
        }
    }

    @Subcommand("list")
    @Cooldown(10)
    fun listFriends(
        actor: Player
    ) {
        clerk.scope.launch {
            val friendsList = friends.getFriends(actor.username)
            actor.sendMessage(
                Component.text(
                    "                                                                              ",
                    NamedTextColor.DARK_GREEN,
                    TextDecoration.STRIKETHROUGH
                )
            )
            actor.sendMessage(Component.text(langConfig.getMessage("friend.list.header"), NamedTextColor.DARK_GREEN))

            if (friendsList.isEmpty()) {
                actor.sendMessage(Component.text(langConfig.getMessage("friend.list.none"), NamedTextColor.GRAY))
                return@launch
            }

            // Create lists to separate online and offline friends
            val onlineFriends = mutableListOf<String>()
            val offlineFriends = mutableListOf<String>()

            // Get the cached account data that should already contain enhanced friend information
            val cachedJson = clerk.lettuce.getAccountCache(actor.username)
            val friendsWithLastSeen = if (cachedJson != null) {
                try {
                    val mapper = jacksonObjectMapper()
                    val accountData = mapper.readValue(cachedJson, Map::class.java)
                    val friendsData = accountData["friends"]

                    // Parse the enhanced friend list that includes last seen data
                    when (friendsData) {
                        is String -> mapper.readValue(friendsData, List::class.java) as? List<Map<*, *>>
                        is List<*> -> friendsData as? List<Map<*, *>>
                        else -> null
                    }
                } catch (e: Exception) {
                    clerk.logger.warn(Component.text("Error parsing enhanced friends from cache: ${e.message}", NamedTextColor.YELLOW))
                    null
                }
            } else null

            // Create a map of username -> last seen timestamp for quick lookups
            val lastSeenMap = mutableMapOf<String, Long>()
            friendsWithLastSeen?.forEach { friendData ->
                val username = friendData["username"]?.toString() ?: return@forEach
                val lastSeen = when (val lastseen = friendData["lastseen"]) {
                    is Number -> lastseen.toLong()
                    is String -> lastseen.toLongOrNull()
                    else -> null
                } ?: 0L

                lastSeenMap[username.lowercase()] = lastSeen
            }

            // Check which friends are online and get their server
            friendsList.forEach { username ->
                val playerOptional = clerk.server.getPlayer(username)
                val isOnline = playerOptional.isPresent &&
                        playerOptional.get().currentServer.isPresent &&
                        playerOptional.get().currentServer.get().serverInfo.name != "auth"

                if (isOnline) {
                    onlineFriends.add(username)
                } else {
                    offlineFriends.add(username)
                }
            }

            // Display online friends first
            if (onlineFriends.isNotEmpty()) {
                var message = Component.text("")
                // Calculate friends per column (ceiling division)
                val friendsPerColumn = (onlineFriends.size + 2) / 3

                // Iterate through rows
                for (row in 0 until friendsPerColumn) {
                    var rowMessage = Component.text("")

                    // Build each row with entries from each column
                    for (col in 0 until 3) {
                        val index = col * friendsPerColumn + row
                        if (index < onlineFriends.size) {
                            val username = onlineFriends[index]
                            val server = clerk.server.getPlayer(username)
                                .map { it.currentServer }
                                .flatMap { it.map { server -> server.serverInfo.name } }
                                .orElse("unknown")

                            rowMessage = rowMessage.append(
                                Component.text(langConfig.getMessage("friend.list.online_format", mapOf("username" to username)), NamedTextColor.GREEN)
                                    .hoverEvent(Component.text(langConfig.getMessage("friend.list.online_hover", mapOf("server" to server))))
                            )
                        }
                    }

                    message = message.append(rowMessage).append(Component.text("\n"))
                }

                actor.sendMessage(message)
            }

            // Then display offline friends
            if (offlineFriends.isNotEmpty()) {
                var message = Component.text("")
                // Calculate friends per column (ceiling division)
                val friendsPerColumn = (offlineFriends.size + 2) / 3

                // Iterate through rows
                for (row in 0 until friendsPerColumn) {
                    var rowMessage = Component.text("")

                    // Build each row with entries from each column
                    for (col in 0 until 3) {
                        val index = col * friendsPerColumn + row
                        if (index < offlineFriends.size) {
                            val username = offlineFriends[index]

                            // Use the cached last seen data instead of fetching it individually
                            val lastSeen = lastSeenMap[username.lowercase()]
                            val lastSeenText = if (lastSeen != null && lastSeen > 0) {
                                friends.formatLastSeen(lastSeen)
                            } else {
                                langConfig.getMessage("friend.list.lastseen_unknown")
                            }

                            rowMessage = rowMessage.append(
                                Component.text(langConfig.getMessage("friend.list.offline_format", mapOf("username" to username)), NamedTextColor.GRAY)
                                    .hoverEvent(Component.text(langConfig.getMessage("friend.list.offline_hover", mapOf("lastSeen" to lastSeenText)), NamedTextColor.GRAY))
                            )
                        }
                    }

                    message = message.append(rowMessage).append(Component.text("\n"))
                }

                actor.sendMessage(message)
            }
        }
    }
}
