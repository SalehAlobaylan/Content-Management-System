FROM golang:1.24.5

WORKDIR /app

COPY go.mod go.sum ./ 

RUN go mod download 

COPY . .

# Build the binary for faster startup and better error handling
RUN go build -o /app/server ./src/main.go

ENV PORT=8080
EXPOSE 8080

CMD ["/app/server"]
