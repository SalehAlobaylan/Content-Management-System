"# Content-Management-System"

## Tech Stack

<p align="left">
  <img src="https://skillicons.dev/icons?i=go" alt="Go" height="50" />
  <img src="https://skillicons.dev/icons?i=postgres" alt="PostgreSQL" height="50" />
  <img src="https://skillicons.dev/icons?i=docker" alt="Docker" height="50" />
</p>


# Install dependencies and run

    go mod download
    go run src/main.go

- Go doc (terminal):
````
  - View all exported APIs: `cd src; go doc -all`
  - View a package: `go doc ./src/models`
  - View a symbol: `go doc ./src/models Post`

Server runs on `http://localhost:8080`
````

## Testing

````
go test -v ./src/tests/integration
````
or non-cached system you can use this command:
      
      go test -v ./src/tests/integration

- Swagger/OpenAPI UI:

  - Spec file: `docs/openapi.yaml`
  - Serve static docs locally: `go run ./cmd/docserver`
  - Open in browser: `http://localhost:8090`

- Swagger UI: `go run ./cmd/docserver` → `http://localhost:8090`
- Go docs: `cd src; go doc -all`
