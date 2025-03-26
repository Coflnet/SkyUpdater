FROM mcr.microsoft.com/dotnet/sdk:9.0 as build
WORKDIR /build
RUN git clone --depth=1 https://github.com/Coflnet/HypixelSkyblock.git dev
RUN mkdir -p /build/skyblock/External/api
RUN git clone --depth=1 https://github.com/Ekwav/Hypixel.NET.git
WORKDIR /build/SkyCommand
COPY SkyUpdater.csproj .
RUN dotnet restore
COPY . .
RUN dotnet test --filter "FullyQualifiedName\!~Parse"
RUN dotnet publish -c release -o /app

FROM mcr.microsoft.com/dotnet/aspnet:9.0
WORKDIR /app

COPY --from=build /app .
ENV ASPNETCORE_URLS=http://+:8000

RUN useradd --uid $(shuf -i 2000-65000 -n 1) app-user
USER app-user

ENTRYPOINT ["dotnet", "SkyUpdater.dll", "--hostBuilder:reloadConfigOnChange=false"]

