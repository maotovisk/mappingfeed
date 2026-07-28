# MappingFeed

Discord bot that mirrors osu! mapping and group events into Discord channels.

It polls osu! APIs, stores events in SQLite, and dispatches formatted embeds to subscribed channels.
It also exposes a minimal HTTP API for recent map/group event views.

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
- HTTP API for recent events:
  - View API with cursor pagination:
    - `GET /api/events/map?limit=20&cursor=<eventId>`
    - `GET /api/events/group?limit=20&cursor=<eventId>`
  - Scalar API reference UI (Swagger-like docs)

## Commands

- `/setup-feed`
  - Interactive flow to create or update a feed subscription in the current channel.
  - This is the only supported creation path.
- `/unsubscribe-feed type:<map|group>`
  - Removes subscription for the selected feed type in the current channel.
- `/feed-status`
  - Shows active subscriptions and filters for the current channel.

## Project Structure

The `MappingFeed` solution contains five projects:

| Project | Responsibility | Depends on |
| --- | --- | --- |
| `MappingFeed.Common` | Shared configuration, entities, records, enums, and interfaces | None |
| `MappingFeed.Data` | EF Core, SQLite repositories, visibility rules, and database backfills | `Common` |
| `MappingFeed.Scraper` | osu! authentication, API access, and event fetching | `Common` |
| `MappingFeed.Discord` | Slash commands, setup interactions, event dispatch, and embeds | `Common`, `Data` |
| `MappingFeed.Web` | Application host, HTTP API, services, and background workers | `Common`, `Data`, `Scraper`, `Discord` |

`MappingFeed.Web` is the executable project. It starts the Discord bot and HTTP
API, registers the other projects, and runs the fetch, dispatch, and backfill
workers.

### Event Flow

1. `MappingFeed.Web/Workers/EventFetcherWorker` runs every
   `Feed.PollIntervalSeconds` and invokes `BeatmapEventsFetcher` and
   `GroupEventsFetcher` from `MappingFeed.Scraper`.
2. The fetchers use `IOsuApiService`, implemented by
   `MappingFeed.Scraper/Services/Osu/OsuApiService`, to request events and related
   metadata from osu!. They parse the responses into `BeatmapsetEvent` and
   `GroupEvent` entities and enrich them with map, user, group, and ruleset data.
3. `BeatmapEventService` and `GroupEventService` remove events already present in
   the database. Their repositories in `MappingFeed.Data/Repositories` then use
   `MappingFeedDbContext` to write the new entities to SQLite.
4. `MappingFeed.Web/Workers/FeedingDispatcherWorker` periodically calls
   `FeedEventsDispatcher`. It loads the map and group subscriptions through
   `SubscribedFeedService`, then routes each subscription to
   `BeatmapEventsDispatcher` or `GroupEventsDispatcher`.
5. Each dispatcher queries events with an `EventId` greater than the
   subscription's `LastEventId`. It applies the subscription's event type,
   ruleset, or group filters before using `FeedEmbedFactory` to build a Discord
   embed.
6. The dispatcher sends the message to the subscribed Discord channel through
   NetCord's `RestClient`. `SubscribedFeedService` advances `LastEventId` after a
   successful send or an intentional filter skip. If sending fails, the cursor
   stays unchanged and the next dispatch cycle retries the event.

## Requirements

- .NET SDK 10.0+
- A Discord bot token
- osu! OAuth app credentials (`client_id`, `client_secret`)

## Configuration

Configuration is loaded from:

1. `MappingFeed.Web/appsettings.json` (optional)
2. `MappingFeed.Web/appsettings.{Environment}.json` (optional)
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

Use [`MappingFeed.Web/appsettings.example.json`](MappingFeed.Web/appsettings.example.json) as a template.

## Local Run

Run the following commands from the repository root.

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
dotnet run --project MappingFeed.Web
```

3. Query recent events:

```bash
curl "http://localhost:5000/api/events/map?limit=10"
curl "http://localhost:5000/api/events/group?limit=10"
```

Each response includes `nextCursor`. For next page, reuse it in the same endpoint as `cursor=<nextCursor>`.

4. Open API docs UI:

```bash
xdg-open http://localhost:5000/scalar
```

5. Build:

```bash
dotnet build
```

## Docker

The Docker hosting files live in `MappingFeed.Web`. Change to that directory:

```bash
cd MappingFeed.Web
```

1. Copy the environment file:

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

SQLite is persisted through the volume defined in
[`MappingFeed.Web/docker-compose.yml`](MappingFeed.Web/docker-compose.yml):

- host: `${HOME}/.local/share/mappingfeed`
- container: `/root/.local/share/mappingfeed`

## Operational Notes
- Bot currently requests only `Guilds` gateway intent.
- Slash commands only work in server channels.
- If a channel is inaccessible or not a text channel, dispatch is skipped for that subscription.
- Map event fetching uses osu! event type filters (`types[]`) for only supported event kinds.
