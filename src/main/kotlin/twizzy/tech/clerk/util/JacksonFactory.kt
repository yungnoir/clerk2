package twizzy.tech.clerk.util

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.readValue
import java.io.File

class JacksonFactory {
    data class DatabaseConfig(
        val postgres: PostgresConfig,
        val redis: RedisConfig
    ) {
        data class PostgresConfig(
            val host: String,
            val port: Int,
            val database: String,
            val user: String,
            val password: String
        )

        data class RedisConfig(
            val host: String,
            val port: Int,
            val password: String? = null
        )
    }

    companion object {
        private val mapper = ObjectMapper(YAMLFactory())
            .registerModule(KotlinModule.Builder().build())

        fun loadDatabaseConfig(filePath: String = "database.yml"): DatabaseConfig {
            val file = File(filePath)
            if (!file.exists()) {
                val defaultConfig = DatabaseConfig(
                    postgres = DatabaseConfig.PostgresConfig(
                        host = "localhost",
                        port = 5432,
                        database = "postgres",
                        user = "postgres",
                        password = "1234"
                    ),
                    redis = DatabaseConfig.RedisConfig(
                        host = "localhost",
                        port = 6379,
                        password = null
                    )
                )
                mapper.writeValue(file, defaultConfig)
            }
            return mapper.readValue(file)
        }
    }
}