FROM golang:1.21 AS build
WORKDIR /app
COPY . ./
RUN go mod download
RUN go build -o /app/binary

FROM debian:stable-slim
WORKDIR /
COPY --from=build /app/binary /heaplog
EXPOSE 8393
ENTRYPOINT ["/heaplog"]