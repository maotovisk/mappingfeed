FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY MappingFeed.csproj ./
RUN dotnet restore MappingFeed.csproj

COPY . .
RUN dotnet publish MappingFeed.csproj -c Release -o /app/publish /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS final
WORKDIR /app

COPY --from=build /app/publish ./

ENV DOTNET_ENVIRONMENT=Production

# SQLite database is stored under LocalApplicationData (/root/.local/share/mappingfeed in this image).
RUN mkdir -p /root/.local/share/mappingfeed

ENTRYPOINT ["dotnet", "MappingFeed.dll"]
