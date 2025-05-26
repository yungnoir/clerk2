package twizzy.tech.clerk.util

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.readValue
import java.io.File

class JacksonFactory {
    data class DatabaseConfig(
        val postgres: PostgresConfig = PostgresConfig(),
        val redis: RedisConfig = RedisConfig(),
        @JsonProperty("auto-sync") val autoSync: String = "10m"
    ) {
        data class PostgresConfig(
            val host: String = "localhost",
            val port: Int = 5432,
            val database: String = "postgres",
            val user: String = "postgres",
            val password: String = "1234"
        )

        data class RedisConfig(
            val host: String = "localhost",
            val port: Int = 6379,
            val username: String = "default",
            val password: String = "mysecretpassword"
        )
    }

    data class Rank(
        val name: String,
        val prefix: String,
        val permissions: List<String>,
        val inheritance: List<String>,
        val weight: Int?,
        @JsonIgnore val users: List<String> = emptyList(),
        @JsonIgnore val isDefault: Boolean = false
    )

    data class RanksConfig(
        val ranks: MutableMap<String, Rank> = mutableMapOf()
    )

    data class LangConfig(
        val messages: MutableMap<String, Any> = mutableMapOf()
    ) {
        fun getMessage(key: String): String {
            val message = messages[key]
            return when (message) {
                is String -> translateColorCodes(message)
                is List<*> -> (message.joinToString("\n") { it.toString() }).let(::translateColorCodes)
                else -> key
            }
        }

        fun getMessage(key: String, replacements: Map<String, String>): String {
            var raw = getMessage(key)
            replacements.forEach { (placeholder, value) ->
                raw = raw.replace("{$placeholder}", value)
            }
            return raw
        }
    }

    companion object {
        val mapper = ObjectMapper(YAMLFactory()
            .configure(com.fasterxml.jackson.dataformat.yaml.YAMLGenerator.Feature.SPLIT_LINES, false)
            .configure(com.fasterxml.jackson.dataformat.yaml.YAMLGenerator.Feature.MINIMIZE_QUOTES, true)
            .configure(com.fasterxml.jackson.dataformat.yaml.YAMLGenerator.Feature.LITERAL_BLOCK_STYLE, true)
        )
            .registerModule(KotlinModule.Builder().build())
            .enable(SerializationFeature.INDENT_OUTPUT)

        @JvmStatic
        fun parseStringArray(input: Any): List<String> {
            return when (input) {
                is List<*> -> input.mapNotNull { it?.toString() }
                is String -> {
                    try {
                        if (input.trim().startsWith("-")) {
                            input.lines()
                                .map { it.trim() }
                                .filter { it.isNotEmpty() && !it.equals("-|", ignoreCase = true) }
                                .map { line ->
                                    if (line.startsWith("-")) line.substring(1).trim() else line
                                }
                        } else {
                            mapper.readValue(input, List::class.java) as? List<String> ?: emptyList()
                        }
                    } catch (e: Exception) {
                        emptyList()
                    }
                }
                else -> emptyList()
            }
        }

        fun loadDatabaseConfig(filePath: String = "plugins/clerk/database.yml"): DatabaseConfig {
            val file = File(filePath)
            if (!file.exists()) {
                file.parentFile?.mkdirs()
                val defaultConfig = DatabaseConfig()
                mapper.writeValue(file, defaultConfig)
            }
            return mapper.readValue(file)
        }

        fun loadRanksConfig(filePath: String = "plugins/clerk/ranks.yml"): RanksConfig {
            val file = File(filePath)
            if (!file.exists()) {
                file.parentFile?.mkdirs()
            }
            return if (file.exists()) {
                try {
                    mapper.readValue(file)
                } catch (e: Exception) {
                    RanksConfig()
                }
            } else {
                RanksConfig()
            }
        }

        fun saveRanksConfig(config: RanksConfig, filePath: String = "plugins/clerk/ranks.yml") {
            try {
                val file = File(filePath)
                file.parentFile?.mkdirs()
                mapper.writeValue(file, config)
            } catch (e: Exception) {
                println("Error saving ranks to YAML: ${e.message}")
            }
        }

        fun translateColorCodes(text: String): String {
            return text.replace("&([0-9a-fk-or])".toRegex()) { matchResult ->
                "§${matchResult.groupValues[1]}"
            }
        }

        fun loadLangConfig(filePath: String = "plugins/clerk/lang.yml"): LangConfig {
            val file = File(filePath)
            if (!file.exists()) {
                file.parentFile?.mkdirs()
                // Create default language config if file doesn't exist
                val defaultConfig = LangConfig(mutableMapOf(

                    // Staff chat messages
                    "staff.prefix" to "&3[Staff] ",
                    "staff.chat_format" to "&b{player}&f: {message}",
                    "staff.chat_enabled" to "&aYou are now talking to the staff channel.",
                    "staff.chat_disabled" to "&eYou are no longer talking to the staff channel.",
                    "staff.listening_enabled" to "&eYou are no longer tuned into the staff channel.",
                    "staff.listening_disabled" to "&aYou are now connected to the staff channel.",
                    "staff.auto_vanish_enabled" to "&aYou have joined the server in vanish mode.",
                    "staff.server_switch" to "&b{player} &fhas connected to&b{to}&ffrom &3{from}&f.",
                    "staff.connected" to "&b{player} &fhas connected to the server.",

                    // Vanish command messages
                    "vanish.now_vanished" to "&aYou are now only visible to staff members.",
                    "vanish.now_visible" to "&eYou are now visible to the public.",
                    "vanish.auto_enabled" to "&aYou will now automatically be vanished.",
                    "vanish.auto_disabled" to "&eYou will no longer automatically be vanished.",

                    // Permission command messages - Help usage
                    "permission.usage.help" to listOf(
                        "&a&l                      Permission Help",
                        "&a  /perm <target> add <permission> [duration] &7- Add a permission",
                        "&a  /perm <target> remove <permission> &7- Remove a permission",
                        "&a  /perm <target> list &7- List player permissions"
                    ),

                    // Permission command - Usage and feedback messages
                    "permission.usage.add" to "&eUsage: /permission <target> add <permission> [duration]",
                    "permission.usage.remove" to "&eUsage: /permission <target> remove <permission>",
                    "permission.usage.list" to "&eUsage: /permission <target> list",
                    "permission.add.no_account" to "&cNo account found for '{target}'.",
                    "permission.add.success" to "&aAdded permission '{permission}' to '{target}'.",
                    "permission.add.success_timed" to "&aAdded permission '{permission}' to '{target}' for {duration}.",
                    "permission.add.failed" to "&cFailed to add permission '{permission}' to '{target}'.",
                    "permission.add.invalid_duration" to "&cInvalid duration format. Use formats like 3d, 5h, 10m.",
                    "permission.remove.no_account" to "&cNo account found for '{target}'.",
                    "permission.remove.success" to "&aRemoved permission '{permission}' from '{target}'.",
                    "permission.remove.failed" to "&cPermission '{permission}' does not exist for '{target}' or could not be removed.",
                    "permission.list.no_account" to "&cNo account found for '{target}'.",
                    "permission.list.none" to "&eNo permissions found for '{target}'.",
                    "permission.list.header" to "&aPermissions for '{target}':",

                    // Grant command messages - Help usage
                    "grant.usage.help" to listOf(
                        "&a&l                         Grant Help",
                        "&a  /grant <target> <rank> [duration] &7- Add a rank to a player",
                        "&a  /grant <target> remove <rank> &7- Remove a rank from a player",
                        "&a  /grants <target> &7- List player ranks"
                    ),

                    // Grant command - Usage and feedback messages
                    "grant.add.usage" to "&eUsage: /grant <target> <rank> [duration]",
                    "grant.add.invalid_duration" to "&cInvalid duration format. Use formats like 3d, 5h, 10m.",
                    "grant.add.rank_not_found" to "&cRank '{rank}' does not exist.",
                    "grant.add.success_perm" to "&aGranted rank '{rank}' to '{target}' permanently.",
                    "grant.add.success_temp" to "&aGranted rank '{rank}' to '{target}' for {duration}.",
                    "grant.add.failed" to "&cFailed to grant rank '{rank}' to '{target}'.",
                    "grant.remove.usage" to "&eUsage: /grant <target> remove <rank>",
                    "grant.remove.rank_not_found" to "&cRank '{rank}' does not exist.",
                    "grant.remove.success" to "&aRemoved rank '{rank}' from '{target}'.",
                    "grant.remove.failed" to "&cFailed to remove rank '{rank}' from '{target}'.",
                    "grants.list.usage" to "&eUsage: /grants <target>",
                    "grants.list.none" to "&e{target} has no ranks.",
                    "grants.list.header" to "&aRanks for '{target}':",
                    "grants.list.entry" to "&a{index}. {rank}",
                    "grants.list.expiry" to " &7(expires in {time})",
                    "grants.list.permanent" to " &7(permanent)",

                    // Rank command messages - Help usage
                    "rank.usage.help" to listOf(
                        "&a&l                         Rank Help",
                        "&a  /rank create <name> &7- Create a new rank",
                        "&a  /rank delete <name> &7- Delete a rank",
                        "&a  /rank list &7- List all ranks",
                        "&a  /rank info <name> &7- Show rank information",
                        "&a  /rank setprefix <name> <prefix> &7- Set rank prefix",
                        "&a  /rank setweight <name> <weight> &7- Set rank weight",
                        "&a  /rank permission add <name> <permission> &7- Add a permission",
                        "&a  /rank permission remove <name> <permission> &7- Remove a permission",
                        "&a  /rank inherit add <name> <parent> &7- Add inheritance",
                        "&a  /rank inherit remove <name> <parent> &7- Remove inheritance",
                        "&a  /ranks save &7- Save ranks to file",
                        "&a  /ranks load &7- Load ranks from file"
                    ),

                    // Rank command - Usage and feedback messages
                    "rank.create.usage" to "&eUsage: /rank create <name>",
                    "rank.create.exists" to "&cRank '{name}' already exists.",
                    "rank.create.success" to "&aRank '{name}' created successfully. Use /rank setprefix and /rank setweight to customize it.",
                    "rank.create.failed" to "&cFailed to create rank '{name}'.",
                    "rank.delete.usage" to "&eUsage: /rank delete <name>",
                    "rank.delete.not_found" to "&cRank '{name}' does not exist.",
                    "rank.delete.success" to "&aRank '{name}' deleted successfully.",
                    "rank.delete.failed" to "&cFailed to delete rank '{name}'. Make sure it's not the default rank.",
                    "rank.list.none" to "&eNo ranks found.",
                    "rank.list.header" to "&a=== Ranks ===",
                    "rank.info.usage" to "&eUsage: /rank info <name>",
                    "rank.info.not_found" to "&cRank '{name}' not found.",
                    "rank.info.header" to "&a=== Rank: {name}{default} ===",
                    "rank.info.default_tag" to " (Default)",
                    "rank.info.prefix" to "&fPrefix: {prefix}",
                    "rank.info.weight" to "&fWeight: {weight}",
                    "rank.info.permissions_header" to "&ePermissions:",
                    "rank.info.permissions_none" to "&7  None",
                    "rank.info.permissions_entry" to "&f  - {permission}",
                    "rank.info.inheritance_header" to "&eInheritance:",
                    "rank.info.inheritance_none" to "&7  None",
                    "rank.info.inheritance_entry" to "&f  - {parent}",
                    "rank.info.users_header" to "&eUsers: {count}",
                    "rank.info.users_list" to "&f  {users}",
                    "rank.setprefix.usage" to "&eUsage: /rank setprefix <name> <prefix>",
                    "rank.setprefix.not_found" to "&cRank '{name}' not found.",
                    "rank.setprefix.success" to "&aPrefix for rank '{name}' set to '{prefix}'.",
                    "rank.setprefix.failed" to "&cFailed to update prefix for rank '{name}'.",
                    "rank.setweight.usage" to "&eUsage: /rank setweight <name> <weight>",
                    "rank.setweight.not_found" to "&cRank '{name}' not found.",
                    "rank.setweight.success" to "&aWeight for rank '{name}' set to {weight}.",
                    "rank.setweight.failed" to "&cFailed to update weight for rank '{name}'.",
                    "rank.permission.add.usage" to "&eUsage: /rank permission add <name> <permission>",
                    "rank.permission.add.not_found" to "&cRank '{name}' not found.",
                    "rank.permission.add.exists" to "&ePermission '{permission}' already exists in rank '{name}'.",
                    "rank.permission.add.success" to "&aPermission '{permission}' added to rank '{name}'.",
                    "rank.permission.add.failed" to "&cFailed to add permission to rank '{name}'.",
                    "rank.permission.remove.usage" to "&eUsage: /rank permission remove <name> <permission>",
                    "rank.permission.remove.not_found" to "&cRank '{name}' not found.",
                    "rank.permission.remove.not_exists" to "&ePermission '{permission}' not found in rank '{name}'.",
                    "rank.permission.remove.success" to "&aPermission '{permission}' removed from rank '{name}'.",
                    "rank.permission.remove.failed" to "&cFailed to remove permission from rank '{name}'.",
                    "rank.inherit.add.usage" to "&eUsage: /rank inherit add <name> <parent>",
                    "rank.inherit.add.not_found" to "&cRank '{name}' not found.",
                    "rank.inherit.add.parent_not_found" to "&cParent rank '{parent}' not found.",
                    "rank.inherit.add.circular" to "&cA rank cannot inherit from itself.",
                    "rank.inherit.add.exists" to "&eRank '{name}' already inherits from '{parent}'.",
                    "rank.inherit.add.success" to "&aRank '{name}' now inherits from '{parent}'.",
                    "rank.inherit.add.failed" to "&cFailed to update inheritance for rank '{name}'.",
                    "rank.inherit.remove.usage" to "&eUsage: /rank inherit remove <name> <parent>",
                    "rank.inherit.remove.not_found" to "&cRank '{name}' not found.",
                    "rank.inherit.remove.not_exists" to "&eRank '{name}' does not inherit from '{parent}'.",
                    "rank.inherit.remove.success" to "&aRemoved inheritance of '{parent}' from rank '{name}'.",
                    "rank.inherit.remove.failed" to "&cFailed to update inheritance for rank '{name}'.",
                    "rank.save.success" to "&aRanks have been saved from database to YAML file.",
                    "rank.save.failed" to "&cFailed to save ranks from database to YAML file.",
                    "rank.load.success" to "&aRanks have been loaded from YAML file to database.",
                    "rank.load.failed" to "&cFailed to load ranks from YAML file to database. Make sure the YAML file exists and contains valid ranks.",

                    // Friend command messages - Help usage
                    "friend.usage.help" to listOf(
                        "&a&l                         Friends Help",
                        "&a  /friend add <username> &7- Send a friend request",
                        "&a  /friend remove <username> &7- Remove a friend",
                        "&a  /friend deny <username> &7- Deny a friend request",
                        "&a  /friend list &7- List your friends",
                        "&a  /friend requests [toggle] &7- View or toggle your friend requests"
                    ),

                    // Friend command - Usage messages
                    "friend.usage.add" to "Usage: /friend add <username>",
                    "friend.usage.remove" to "Usage: /friend remove <username>",
                    "friend.usage.deny" to "Usage: /friend deny <username>",

                    // Friend command - Add friend messages
                    "friend.add.self" to "&eYou can't add yourself as a friend.",
                    "friend.add.request_sent" to "&a\uD83D\uDC65 You have sent a friend request to {target}.",
                    "friend.add.incoming_request" to "&a\uD83D\uDC65 Friend request from {player}",
                    "friend.add.accept_hover" to "☑",
                    "friend.add.deny_hover" to "ⓧ",
                    "friend.add.now_friends" to "&a\n\uD83D\uDC65 You and {target} are now friends!\n",
                    "friend.add.already_friends" to "&eYou're already friends with {target}.",
                    "friend.add.request_already_sent" to "&eYou have already sent a friend request to {target}.",
                    "friend.add.user_not_found" to "&cThe player '{target}' does not exist.",
                    "friend.add.requests_disabled" to "&c{target} is not accepting friend requests.",
                    "friend.add.error" to "&cAn error occurred while processing your friend request.",

                    // Friend command - Remove friend messages
                    "friend.remove.removed" to "&eYou and {target} are no longer friends.",
                    "friend.remove.request_cancelled" to "&eYou revoked your friend request to {target}.",
                    "friend.remove.not_friends" to "&cYou aren't friends with {target} and didn't send a request.",
                    "friend.remove.error" to "&cThere was a problem removing {target} as a friend.",

                    // Friend command - Deny friend messages
                    "friend.deny.success" to "&eYou have denied {target}'s friend request.",
                    "friend.deny.no_request" to "&cYou don't have a friend request from {target}.",

                    // Friend command - Requests messages
                    "friend.requests.header" to "&2                     Friend Requests",
                    "friend.requests.none" to "&7You have no friend requests.",
                    "friend.requests.incoming_header" to "&a\n  Incoming Requests:",
                    "friend.requests.incoming_format" to "&7\uD83D\uDCE8 &2{username} ",
                    "friend.requests.accept_button" to "&a&l[Accept]",
                    "friend.requests.accept_hover" to "Click to accept",
                    "friend.requests.deny_button" to "&c&l[Deny]",
                    "friend.requests.deny_hover" to "Click to deny",
                    "friend.requests.outgoing_header" to "&a\n  Outgoing Requests:",
                    "friend.requests.outgoing_format" to "&7✉ &a{username}  ",
                    "friend.requests.cancel_button" to "&c[Cancel]",
                    "friend.requests.cancel_hover" to "Click to cancel request",
                    "friend.requests.toggle_info" to "&7\nUse /friend requests toggle to turn requests on/off",
                    "friend.requests.toggle_off" to "&eYou will no longer receive friend requests.",
                    "friend.requests.toggle_on" to "&aYou can now receive friend requests.",

                    // Friend command - List friends messages
                    "friend.list.header" to "&2\n                     Friends List",
                    "friend.list.none" to "&7You don't have any friends yet.",
                    "friend.list.online_format" to "&a    \uD83D\uDC65 {username}    ",
                    "friend.list.online_hover" to "Playing on {server}",
                    "friend.list.offline_format" to "&8    \uD83D\uDE34 &7{username}    ",
                    "friend.list.offline_hover" to "Last seen: {lastSeen}",
                    "friend.list.lastseen_unknown" to "Unknown",

                    // Lobby command messages
                    "lobby.sending" to "&eSending you to the lobby...",
                    "lobby.not_found" to "&cLobby server not found.",

                    // Login command messages
                    "login.usage" to "&eUsage: /login <username> <password>",
                    "login.already_logged_in" to "&cYou are already logged in.",
                    "login.successful" to "&aLogin successful! Welcome back.",
                    "login.logged_out" to "&eYou have been logged out.",

                    // Register command messages
                    "register.usage" to "&eUsage: /register <username> <password>",
                    "register.already_started" to "&eYou have already started registration. Please type your password in chat to confirm.",
                    "register.already_logged_in" to "&cYou are already logged into an account, please log out.",
                    "register.confirm_password" to "&ePlease type your password in chat to confirm registration.",
                    "register.successful" to "&aRegistration successful! You have created a new account with the username '{username}'.\nNow you can use /login with your credentials.",
                    "register.failed" to "&cRegistration failed due to an error: {error}. Please try again later.",
                    "register.passwords_not_match" to "&cRegistration failed. Passwords didn't match. Please try again.",
                    "register.no_registration" to "&cRegistration failed. No registration in progress. Please try again.",

                    // Register validation messages
                    "register.validate.username_length" to "&cYour username must be between 3 and 16 characters.",
                    "register.validate.username_chars" to "&cYour username must be alphanumerical.",
                    "register.validate.username_inappropriate" to "&cYour username contains inappropriate or blocked words.",
                    "register.validate.password_length" to "&cYour password must be at least 6 characters long.",
                    "register.validate.password_weak" to "&cYour password is too weak, try making it more complex.",
                    "register.validate.username_taken" to "&cThat username is already taken.",

                    // Account command messages - Help usage
                    "account.usage.help" to listOf(
                        "&a&l                         Account Help",
                        "&a  /account reset &7- Change your password",
                        "&a  /account autolock &7- Toggle automatic logout",
                        "&a  /account logins &7- View login history"
                    ),


                    // Account reset messages
                    "account.reset.already_in_process" to "&eYou are already in the process of resetting your password. Please follow the prompts in chat.",
                    "account.reset.start" to "&ePlease type your current password in chat to begin the reset process. Type 'cancel' to cancel.",
                    "account.reset.cancelled" to "&cPassword reset cancelled.",
                    "account.reset.incorrect_password" to "&cYour current password is incorrect. Please try again or type 'cancel' to cancel.",
                    "account.reset.enter_new" to "&ePlease type your new password in chat. Type 'cancel' to cancel.",
                    "account.reset.password_too_short" to "&cYour password must be at least 6 characters long. Please try again.",
                    "account.reset.password_too_weak" to "&cYour password is too weak, try making it more complex. Please try again.",
                    "account.reset.password_same" to "&cThe new password must be different from the old password. Please try again.",
                    "account.reset.confirm_new" to "&ePlease type your new password again to confirm.",
                    "account.reset.passwords_dont_match" to "&cPasswords don't match. Password reset cancelled.",
                    "account.reset.success" to "&aYour password has been changed successfully. You have been logged out for security reasons.",

                    // Account logins messages
                    "account.logins.no_history" to "&eNo login history found.",
                    "account.logins.header" to "&aLast 10 logins:",
                    "account.logins.format" to "&b{ago} on {platform} from {country}, {region}",

                    // Account autolock messages
                    "account.autolock.enabled" to "&aAuto-lock enabled. You will be logged out every time you disconnect.",
                    "account.autolock.disabled" to "&aAuto-lock disabled. You will stay logged in after disconnecting."
                ))
                mapper.writeValue(file, defaultConfig)
                return defaultConfig
            }
            return try {
                mapper.readValue(file)
            } catch (e: Exception) {
                println("Error loading language config: ${e.message}")
                LangConfig()
            }
        }

        fun saveLangConfig(config: LangConfig, filePath: String = "plugins/clerk/lang.yml") {
            try {
                val file = File(filePath)
                file.parentFile?.mkdirs()
                mapper.writeValue(file, config)
            } catch (e: Exception) {
                println("Error saving language config to YAML: ${e.message}")
            }
        }
    }
}
