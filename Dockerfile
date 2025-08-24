FROM golang:1.25-bookworm AS build
WORKDIR /app
COPY . ./
RUN go build -o /app/binary

FROM debian:bookworm-slim
RUN apt-get update
RUN apt-get install ugrep
COPY --from=build /app/binary /heaplog
EXPOSE 8393
ENTRYPOINT ["/heaplog"]
CMD ["run"]