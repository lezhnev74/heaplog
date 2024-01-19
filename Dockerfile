FROM golang:1.21-bookworm AS build
WORKDIR /app
COPY . ./
RUN go build -o /app/binary

FROM debian:bookworm-slim
COPY --from=build /app/binary /heaplog
EXPOSE 8393
ENTRYPOINT ["/heaplog"]