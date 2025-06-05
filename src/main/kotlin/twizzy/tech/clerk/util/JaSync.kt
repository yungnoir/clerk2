package twizzy.tech.clerk.util

import com.github.jasync.sql.db.QueryResult
import com.github.jasync.sql.db.pool.ConnectionPool
import com.github.jasync.sql.db.postgresql.PostgreSQLConnectionBuilder
import kotlinx.coroutines.future.await
import twizzy.tech.clerk.Clerk
import java.util.concurrent.atomic.AtomicReference

class JaSync(val clerk: Clerk) {

    val config = JacksonFactory.loadDatabaseConfig()
    private val postgres = config.postgres
    
    // Use lazy initialization for the connection pool to ensure it's only created once
    private val connectionPoolRef = AtomicReference<ConnectionPool<*>>()
    
    // Getter for the connection pool - creates it only if needed
    val connectionPool: ConnectionPool<*>
        get() {
            return connectionPoolRef.get() ?: createConnectionPool().also {
                connectionPoolRef.set(it)
            }
        }
    
    private fun createConnectionPool(): ConnectionPool<*> {
        return PostgreSQLConnectionBuilder.createConnectionPool(
            "jdbc:postgresql://${postgres.host}:${postgres.port}/${postgres.database}?user=${postgres.user}&password=${postgres.password}"
        )
    }
    
    // Cleanup method to properly disconnect when plugin is disabled
    fun shutdown() {
        connectionPoolRef.get()?.disconnect()
    }

    suspend fun executeQuery(query: String): QueryResult {
        // Use the plugin's coroutine scope to manage lifecycle properly
        return try {
            connectionPool.sendQuery(query).await()
        } catch (e: Exception) {
            clerk.logger.error("Database operation failed: ${e.message}")
            throw e
        }
    }

    // Method to execute multiple queries in a batch to reduce connection overhead
    suspend fun executeBatch(queries: List<String>): List<QueryResult> {
        if (queries.isEmpty()) return emptyList()

        // Use structured concurrency for better error handling
        return try {
            queries.map { query ->
                connectionPool.sendQuery(query).await()
            }
        } catch (e: Exception) {
            clerk.logger.error("Batch DB operation failed: ${e.message}")
            throw e
        }
    }

    suspend fun ensurePgcryptoExtensionEnabled(): String {
        val enableExtensionQuery = """
        CREATE EXTENSION IF NOT EXISTS pgcrypto;
    """.trimIndent()
        executeQuery(enableExtensionQuery)
        return "pgcrypto extension ensured."
    }

    suspend fun initializeDatabase(): List<Clerk.DatabaseMessage> {
        val messages = mutableListOf<Clerk.DatabaseMessage>()
        try {
            messages.add(Clerk.DatabaseMessage("Ensuring pgcrypto extension is enabled...", Clerk.MessageType.ATTEMPT))
            val pgcryptoMsg = ensurePgcryptoExtensionEnabled()
            messages.add(Clerk.DatabaseMessage(pgcryptoMsg, Clerk.MessageType.SUCCESS))

            // Initialize accounts table
            messages.addAll(ensureTableWithSchema("accounts", PostSchema.AccountsTableSchema.columns))
            
            // Initialize ranks table
            messages.addAll(ensureTableWithSchema("ranks", PostSchema.RanksTableSchema.columns))

            messages.add(Clerk.DatabaseMessage("Database initialization completed successfully", Clerk.MessageType.SUCCESS))
        } catch (e: Exception) {
            messages.add(Clerk.DatabaseMessage("Error during database initialization: ${e.message}", Clerk.MessageType.ERROR))
        }
        return messages
    }
    
    private suspend fun ensureTableWithSchema(tableName: String, schemaColumns: List<PostSchema.TableColumn>): List<Clerk.DatabaseMessage> {
        val messages = mutableListOf<Clerk.DatabaseMessage>()
        
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
            messages.add(Clerk.DatabaseMessage("Creating $tableName table...", Clerk.MessageType.ATTEMPT))
            val columnsDef = schemaColumns.joinToString(",\n") {
                "${it.name} ${it.type}${if (it.constraints.isNotBlank()) " ${it.constraints}" else ""}"
            }
            val createTableQuery = """
                CREATE TABLE $tableName (
                    $columnsDef
                );
            """.trimIndent()
            executeQuery(createTableQuery)
            messages.add(Clerk.DatabaseMessage("Successfully created $tableName table", Clerk.MessageType.SUCCESS))
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
            messages.add(Clerk.DatabaseMessage("Adding column '${col.name}' to $tableName table...", Clerk.MessageType.ATTEMPT))
            val alterQuery = "ALTER TABLE $tableName ADD COLUMN ${col.name} ${col.type}${if (col.constraints.isNotBlank()) " ${col.constraints}" else ""};"
            executeQuery(alterQuery)
            messages.add(Clerk.DatabaseMessage("Successfully added column '${col.name}'", Clerk.MessageType.SUCCESS))
        }

        // Drop extra columns
        for (col in columnsToDrop) {
            messages.add(Clerk.DatabaseMessage("Dropping column '$col' from $tableName table...", Clerk.MessageType.ATTEMPT))
            val alterQuery = "ALTER TABLE $tableName DROP COLUMN $col;"
            executeQuery(alterQuery)
            messages.add(Clerk.DatabaseMessage("Successfully dropped column '$col'", Clerk.MessageType.SUCCESS))
        }

        messages.add(Clerk.DatabaseMessage("Database schema for '$tableName' is up to date.", Clerk.MessageType.SUCCESS))
        return messages
    }

    suspend fun insertNewAccount(values: Map<String, Any?>): QueryResult {
        val schemaColumns = PostSchema.AccountsTableSchema.columns
        val columnsToInsert = mutableListOf<String>()
        val valuesToInsert = mutableListOf<String>()
        
        for (column in schemaColumns) {
            // Only include columns that have a value or have a default in the database
            val value = values[column.name]
            if (value != null || !column.constraints.contains("DEFAULT", ignoreCase = true)) {
                columnsToInsert.add(column.name)
                valuesToInsert.add(formatValueForSql(column, value))
            }
        }
        
        val insertQuery = """
            INSERT INTO accounts (${columnsToInsert.joinToString(", ")})
            VALUES (${valuesToInsert.joinToString(", ")})
            ON CONFLICT (username) DO NOTHING;
        """.trimIndent()

        return executeQuery(insertQuery)
    }

    /**
     * Formats a value for SQL insertion based on the column type
     */
    private fun formatValueForSql(column: PostSchema.TableColumn, value: Any?): String {
        val type = column.type.lowercase()
        
        return when {
            // Special case for password to allow SQL function
            column.name == "password" && value is String && value.startsWith("crypt(") -> value
            
            // Handle null values
            value == null -> when {
                type.contains("jsonb") -> "'[]'::jsonb"
                type.contains("timestamp") -> "NOW()"
                type.contains("int") || type.contains("numeric") || type.contains("serial") -> "0"
                type.contains("boolean") -> "FALSE"
                else -> "NULL"
            }
            
            // Handle different data types
            type.contains("jsonb") -> formatJsonbValue(value)
            type.contains("timestamp") -> formatTimestampValue(value)
            type.contains("boolean") -> formatBooleanValue(value)
            type.contains("int") || type.contains("numeric") || type.contains("serial") -> value.toString()
            
            // Default string handling for text, varchar, etc.
            value is String -> "'${value.replace("'", "''")}'"
            value is List<*> -> "'${value.joinToString(",")}'"
            else -> "'${value.toString().replace("'", "''")}'"
        }
    }
    
    private fun formatJsonbValue(value: Any?): String {
        return when (value) {
            is String -> {
                // If it's already in JSON format, just add the cast
                if (value.startsWith("[") || value.startsWith("{")) {
                    "'$value'::jsonb"
                } else {
                    // Escape the string as a JSON string
                    "'\"${value.replace("\"", "\\\"")}\"'::jsonb"
                }
            }
            // Lists get converted to JSON arrays
            is List<*> -> {
                val jsonArray = value.joinToString(",", "[", "]") { 
                    if (it is String) "\"${it.replace("\"", "\\\"")}\"" else it.toString()
                }
                "'$jsonArray'::jsonb"
            }
            else -> "'[]'::jsonb"
        }
    }
    
    private fun formatTimestampValue(value: Any?): String {
        return when (value) {
            is String -> {
                if (value.equals("now()", ignoreCase = true)) {
                    "NOW()"
                } else {
                    "'$value'"
                }
            }
            else -> "NOW()"
        }
    }
    
    private fun formatBooleanValue(value: Any?): String {
        return when (value) {
            is Boolean -> if (value) "TRUE" else "FALSE"
            is String -> if (value.equals("true", ignoreCase = true)) "TRUE" else "FALSE"
            else -> "FALSE"
        }
    }
}
