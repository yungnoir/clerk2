package twizzy.tech.clerk.util

class PostSchema {
    data class TableColumn(
        val name: String,
        val type: String,
        val constraints: String = ""
    )

    object AccountsTableSchema {
        val columns = listOf(
            TableColumn("username", "VARCHAR(32)", "PRIMARY KEY"),
            TableColumn("password", "VARCHAR(255)", "NOT NULL"),
            TableColumn("platform", "VARCHAR(16)", "NOT NULL"),
            TableColumn("ip_address", "JSONB", "DEFAULT '[]'::jsonb"),
            TableColumn("uuids", "JSONB", "DEFAULT '[]'::jsonb"),
            TableColumn("registered_date", "TIMESTAMP WITH TIME ZONE", "DEFAULT NOW()"),
            TableColumn("logins", "JSONB", "DEFAULT '[]'::jsonb"),
            TableColumn("logged_out", "BOOLEAN", "DEFAULT FALSE"),
            TableColumn("auto_lock", "BOOLEAN", "DEFAULT FALSE"),
            TableColumn("failed_attempts", "INT", "DEFAULT 0"),
            TableColumn("lock_until", "TIMESTAMP WITH TIME ZONE"),
            TableColumn("lock_reason", "VARCHAR(128)", "DEFAULT ''"), // <-- Added here
            TableColumn("locked", "BOOLEAN", "DEFAULT FALSE"),
            TableColumn("country", "VARCHAR(64)", "DEFAULT ''"),
            TableColumn("region", "VARCHAR(64)", "DEFAULT ''"),
            TableColumn("permissions", "JSONB", "DEFAULT '[]'::jsonb"),
            TableColumn("ranks", "JSONB", "DEFAULT '[{\"rank\": \"Default\"}]'::jsonb"),
            TableColumn("friends", "JSONB", "DEFAULT '[]'::jsonb"),
            TableColumn("incoming_requests", "JSONB", "DEFAULT '[]'::jsonb"),
            TableColumn("outgoing_requests", "JSONB", "DEFAULT '[]'::jsonb"),
            TableColumn("last_seen", "TIMESTAMP WITH TIME ZONE", "DEFAULT NOW()"),
            TableColumn("settings", "JSONB", "DEFAULT '[]'::jsonb"),
            TableColumn("crossplay", "BOOLEAN", "DEFAULT FALSE")
        )
    }
    
    object RanksTableSchema {
        val columns = listOf(
            TableColumn("name", "VARCHAR(32)", "PRIMARY KEY"),
            TableColumn("prefix", "VARCHAR(64)", "DEFAULT ''"),
            TableColumn("permissions", "JSONB", "DEFAULT '[]'::jsonb"),
            TableColumn("inheritance", "JSONB", "DEFAULT '[]'::jsonb"),
            TableColumn("weight", "INT", "DEFAULT 0"),
            TableColumn("users", "JSONB", "DEFAULT '[]'::jsonb")
        )
    }
}
