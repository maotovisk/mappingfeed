# MappingFeed

Discord bot that mirrors osu! mapping and group events into Discord channels.

It polls osu! APIs, stores events in SQLite, and dispatches formatted embeds to subscribed channels.

## Features

- Map feed events:
  - `nominate`
  - `nomination_reset`
  - `qualify`
  - `disqualify`
  - `rank`
  - `unrank`
- Group membership feed events:
  - user add/remove history from osu! groups
- Per-channel feed subscriptions with filters:
  - map rulesets (`osu`, `taiko`, `catch`, `mania`)
  - map event types
  - group ids
- Interactive setup via Discord components (`/setup-feed`)
- Persistent SQLite state (subscriptions, fetched events, cursor per channel/feed)

## Commands

- `/setup-feed`
  - Interactive flow to create or update a feed subscription in the current channel.
  - This is the only supported creation path.
- `/unsubscribe-feed type:<map|group>`
  - Removes subscription for the selected feed type in the current channel.
- `/feed-status`
  - Shows active subscriptions and filters for the current channel.

## Architecture

- `FeedFetchingWorker`
  - Polls osu! endpoints and writes new events into local DB.
- `FeedSendingWorker`
  - Reads pending events per subscription, applies filters, sends Discord messages, advances `LastEventId` cursor.
- `FeedEmbedFactory`
  - Builds map/group embeds and enriches text with osu! user/beatmapset data.
- `MappingFeedDbContext` + `DatabaseSchemaUpdater`
  - Maintains SQLite schema and lightweight column backfills.

## Requirements

- .NET SDK 10.0+
- A Discord bot token
- osu! OAuth app credentials (`client_id`, `client_secret`)

## Configuration

Configuration is loaded from:

1. `appsettings.json` (optional)
2. `appsettings.{Environment}.json` (optional)
3. Environment variables (override JSON)

Main sections:

- `Discord`
  - `Token` (required)
- `Osu`
  - `BaseUrl` (default `https://osu.ppy.sh`)
  - `ClientId` (required)
  - `ClientSecret` (required)
- `Feed`
  - `PollIntervalSeconds` (default `30`)
  - `DispatchIntervalSeconds` (configured value is clamped to minimum `180` in sender)
  - `EventsBatchSize` (default `25`)
  - `DispatchBatchSize` (configured value is clamped to max `10` in sender)
  - `ApiCacheMinutes` (default `10`, clamped in API client to `5..20`)

Use [`appsettings.example.json`](/home/maoto/dev/MappingFeed/appsettings.example.json) as template.

## Local Run

1. Create environment variables (recommended), for example:

```bash
export Discord__Token="<discord-token>"
export Osu__ClientId="<osu-client-id>"
export Osu__ClientSecret="<osu-client-secret>"
# optional:
export Osu__BaseUrl="https://osu.ppy.sh"
```

2. Run:

```bash
dotnet run
```

3. Build:

```bash
dotnet build
```

## Docker

1. Copy environment file:

```bash
cp .env.example .env
```

2. Fill required values in `.env`:

- `DISCORD_TOKEN`
- `OSU_CLIENT_ID`
- `OSU_CLIENT_SECRET`

3. Start:

```bash
docker compose up -d --build
```

SQLite is persisted through the volume defined in [`docker-compose.yml`](/home/maoto/dev/MappingFeed/docker-compose.yml):

- host: `${HOME}/.local/share/mappingfeed`
- container: `/root/.local/share/mappingfeed`

## Operational Notes
- Bot currently requests only `Guilds` gateway intent.
- Slash commands only work in server channels.
- If a channel is inaccessible or not a text channel, dispatch is skipped for that subscription.
- Map event fetching uses osu! event type filters (`types[]`) for only supported event kinds.
