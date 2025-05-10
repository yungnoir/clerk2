📄 Project Overview
Clerk is a custom account system for Minecraft designed to unify Java and Bedrock players under any username they choose by running Velocity in offline mode. It stores credentials securely in PostgreSQL and offers robust protection against brute‑force and suspicious login attempts. Written in Kotlin, Clerk integrates smoothly with Velocity, Folia, and Minestom to enable seamless cross‑play.

🚀 Features
Account Management

. Register new accounts, log in, reset passwords, and auto‑logout inactive users.

. Detect and block suspicious login patterns with brute‑force protection.

Configuration and Filtering

. Human‑friendly YAML config via Jackson’s YAML dataformat module 

. Auto‑moderation and text filter using SymSpell plus leetspeak algorithm for wider accuracy. 

Database

. Asynchronous PostgreSQL manager with on‑the‑fly schema builder ensures high throughput and reliability 

Proxy Support

. Built for Velocity proxy (offline mode) to allow arbitrary usernames and unify Java/Bedrock players

🎮 Usage:
. /register <username> <password> (Reenter password in chat to confirm)

. /login <username> <password> (Rejoin the proxy to complete the login process)

. /account reset (Enter old and new password in chat to reset)

. /account logins (List the last 10 logins on your account)

. /account autolock (Automatically log out of your account each session)

Suspicious activity (e.g. rapid failed logins and out of region logins) triggers alerts in console and temporary lockouts.

🧪 Planned Features
. Redis Caching for session acceleration

. Punishments & Permissions system

. Social: Friends list, Discord & Forums webhooks

. Economy: Account levels and balances

. Staff Utilities: Moderation commands and audit logs
