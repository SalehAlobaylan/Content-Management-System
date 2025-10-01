FROM golang:1.24.5

WORKDIR /app

COPY go.mod go.sum ./ 

# RUN go mod download && go mod verify
RUN go mod download 

COPY . .

ENV PORT=8080
EXPOSE 8080

CMD ["go","run","src/main.go"]
