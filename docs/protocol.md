# NovaKV wire protocol

NovaKV uses a deliberately small, line-oriented TCP protocol. Requests are UTF-8 command lines terminated by `\n`.

Commands:

- `PING`
- `SET <key> <value>` — value may contain spaces.
- `SETEX <key> <ttl_ms> <value>` — TTL is measured in milliseconds.
- `GET <key>`
- `DEL <key>`
- `EXISTS <key>`
- `TTL <key>` — returns remaining milliseconds, `-1` for no expiry, `-2` for missing.
- `INCR <key> [delta]`
- `STATS`
- `CHECKPOINT`
- `QUIT`

Responses are RESP-inspired but intentionally simpler:

- `+OK` — simple string
- `:<integer>` — integer
- `$<length>\n<payload>` — bulk string
- `$-1` — missing bulk value
- `-ERR <message>` — error
