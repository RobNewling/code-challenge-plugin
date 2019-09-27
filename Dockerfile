# I had to build my implementation first
FROM mcr.microsoft.com/dotnet/core/runtime:3.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/core/sdk:3.0 AS build
WORKDIR /src
COPY NaveegoGrpcPlugin/NaveegoGrpcPlugin/NaveegoGrpcPlugin.csproj NaveegoGrpcPlugin/
RUN dotnet restore NaveegoGrpcPlugin/NaveegoGrpcPlugin.csproj
COPY . .
WORKDIR /src/NaveegoGrpcPlugin/NaveegoGrpcPlugin
RUN dotnet build NaveegoGrpcPlugin.csproj -c Release -o /app

FROM build AS publish
RUN dotnet publish NaveegoGrpcPlugin.csproj -c Release -o /app -r linux-x64 --self-contained true

FROM base AS final
WORKDIR /app
COPY --from=publish /app .

FROM golang:1.11-stretch

WORKDIR /code-challenge-plugin
#Then copy it into the go environment
COPY --from=final /app .

ADD go.mod .
ADD go.sum .

RUN go mod download

ADD ./plugin/ ./plugin
ADD ./data/ ./data

ADD host.go .

ENTRYPOINT ["go", "run", "host.go"]

CMD ["./NaveegoGrpcPlugin"]

#ENTRYPOINT ["/bin/sh"]
