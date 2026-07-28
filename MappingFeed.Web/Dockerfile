FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY MappingFeed.slnx ./
COPY MappingFeed.Common/MappingFeed.Common.csproj MappingFeed.Common/
COPY MappingFeed.Data/MappingFeed.Data.csproj MappingFeed.Data/
COPY MappingFeed.Scraper/MappingFeed.Scraper.csproj MappingFeed.Scraper/
COPY MappingFeed.Discord/MappingFeed.Discord.csproj MappingFeed.Discord/
COPY MappingFeed.Web/MappingFeed.Web.csproj MappingFeed.Web/
RUN dotnet restore MappingFeed.slnx

COPY . .
RUN dotnet publish MappingFeed.Web/MappingFeed.Web.csproj -c Release -o /app/publish /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS final
WORKDIR /app

COPY --from=build /app/publish ./

ENV DOTNET_ENVIRONMENT=Production
ENV Feed__EnableApiBackfillWorker=true
ENV Feed__ApiBackfillStartupDelaySeconds=20
ENV Feed__ApiBackfillRepeatIntervalMinutes=0
ENV Feed__ApiBackfillThrottleMilliseconds=1000
ENV Feed__ApiBackfillBatchSize=128

# SQLite database is stored under LocalApplicationData (/root/.local/share/mappingfeed in this image).
RUN mkdir -p /root/.local/share/mappingfeed

ENTRYPOINT ["dotnet", "MappingFeed.Web.dll"]
