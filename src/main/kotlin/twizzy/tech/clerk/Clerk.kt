package twizzy.tech.clerk;

import com.google.inject.Inject
import com.velocitypowered.api.event.Subscribe
import com.velocitypowered.api.event.proxy.ProxyInitializeEvent
import com.velocitypowered.api.plugin.Plugin
import com.velocitypowered.api.proxy.ProxyServer
import kotlinx.coroutines.runBlocking
import twizzy.tech.clerk.util.JaSync
import twizzy.tech.clerk.util.JaSync.MessageType
import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.format.NamedTextColor;
import net.kyori.adventure.text.logger.slf4j.ComponentLogger
import revxrsal.commands.velocity.VelocityLamp
import revxrsal.commands.velocity.VelocityVisitors.brigadier

@Plugin(
    id = "clerk", name = "clerk", authors = ["mightbmax"], version = BuildConstants.VERSION
)
class Clerk @Inject constructor(val logger: ComponentLogger, val server: ProxyServer) {


    private val jaSync = JaSync() // Initialize JaSync instance
    private val lamp = VelocityLamp.builder(this, server).build() // Initialize Lamp instance

    init {
        logger.info(Component.text("Attempting to connect to the account database...", NamedTextColor.YELLOW))
        
        runBlocking { 
            val messages = jaSync.initializeDatabase()
            messages.forEach { msg ->
                val color = when (msg.type) {
                    MessageType.SUCCESS -> NamedTextColor.GREEN
                    MessageType.ATTEMPT -> NamedTextColor.YELLOW
                    MessageType.ERROR -> NamedTextColor.RED
                }
                logger.info(Component.text(msg.message, color))
            }
            logger.info(Component.text("Successfully connected to the database!", NamedTextColor.GREEN))
        }

        // Register Commands
        lamp.register()
        lamp.accept(brigadier(server))
    }

    @Subscribe
    fun onProxyInitialization(event: ProxyInitializeEvent) {
        println("initialized clerk")
    }
}
