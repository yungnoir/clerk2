package twizzy.tech.clerk.util

import com.github.jasync.sql.db.QueryResult
import com.github.jasync.sql.db.pool.ConnectionPool
import com.github.jasync.sql.db.postgresql.PostgreSQLConnectionBuilder
import kotlinx.coroutines.future.await

class JaSync {

    enum class MessageType {
        SUCCESS, ATTEMPT, ERROR
    }

    data class DatabaseMessage(val message: String, val type: MessageType)

    val config = JacksonFactory.loadDatabaseConfig()
    val postgres = config.postgres
    val connectionPool: ConnectionPool<*> = PostgreSQLConnectionBuilder.createConnectionPool(
        "jdbc:postgresql://${postgres.host}:${postgres.port}/${postgres.database}?user=${postgres.user}&password=${postgres.password}"
    )

    suspend fun executeQuery(query: String): QueryResult {
        return connectionPool.sendQuery(query).await()
    }

    suspend fun ensurePgcryptoExtensionEnabled(): String {
        val enableExtensionQuery = """
        CREATE EXTENSION IF NOT EXISTS pgcrypto;
    """.trimIndent()
        executeQuery(enableExtensionQuery)
        return "pgcrypto extension ensured."
    }

    suspend fun initializeDatabase(): List<DatabaseMessage> {
        val messages = mutableListOf<DatabaseMessage>()
        try {
            messages.add(DatabaseMessage("Ensuring pgcrypto extension is enabled...", MessageType.ATTEMPT))
            val pgcryptoMsg = ensurePgcryptoExtensionEnabled()
            messages.add(DatabaseMessage(pgcryptoMsg, MessageType.SUCCESS))

            val tableName = "accounts"
            val schemaColumns = PostSchema.AccountsTableSchema.columns

            // Check if table exists
            val checkTableQuery = """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = '$tableName'
                );
            """.trimIndent()
            val tableExists = executeQuery(checkTableQuery).rows.first().getBoolean(0) ?: false

            if (!tableExists) {
                messages.add(DatabaseMessage("Creating $tableName table...", MessageType.ATTEMPT))
                val columnsDef = schemaColumns.joinToString(",\n") {
                    "${it.name} ${it.type}${if (it.constraints.isNotBlank()) " ${it.constraints}" else ""}"
                }
                val createTableQuery = """
                    CREATE TABLE $tableName (
                        $columnsDef
                    );
                """.trimIndent()
                executeQuery(createTableQuery)
                messages.add(DatabaseMessage("Successfully created $tableName table", MessageType.SUCCESS))
                messages.add(DatabaseMessage("Database initialization completed successfully", MessageType.SUCCESS))
                return messages
            }

            // Get current columns from DB
            val getColumnsQuery = """
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = '$tableName';
            """.trimIndent()
            val result = executeQuery(getColumnsQuery)
            val dbColumns = result.rows.map { row ->
                row.getString("column_name") ?: ""
            }.toSet()

            val schemaColumnNames = schemaColumns.map { it.name }.toSet()

            // Columns to add
            val columnsToAdd = schemaColumns.filter { it.name !in dbColumns }
            // Columns to drop
            val columnsToDrop = dbColumns.filter { it !in schemaColumnNames }

            // Add missing columns
            for (col in columnsToAdd) {
                messages.add(DatabaseMessage("Adding column '${col.name}' to $tableName table...", MessageType.ATTEMPT))
                val alterQuery = "ALTER TABLE $tableName ADD COLUMN ${col.name} ${col.type}${if (col.constraints.isNotBlank()) " ${col.constraints}" else ""};"
                executeQuery(alterQuery)
                messages.add(DatabaseMessage("Successfully added column '${col.name}'", MessageType.SUCCESS))
            }

            // Drop extra columns
            for (col in columnsToDrop) {
                messages.add(DatabaseMessage("Dropping column '$col' from $tableName table...", MessageType.ATTEMPT))
                val alterQuery = "ALTER TABLE $tableName DROP COLUMN $col;"
                executeQuery(alterQuery)
                messages.add(DatabaseMessage("Successfully dropped column '$col'", MessageType.SUCCESS))
            }

            messages.add(DatabaseMessage("Database schema for '$tableName' is up to date.", MessageType.SUCCESS))
        } catch (e: Exception) {
            messages.add(DatabaseMessage("Error during database initialization: ${e.message}", MessageType.ERROR))
        }
        return messages
    }
}
