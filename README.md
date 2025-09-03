"# Content-Management-System"

## Tech Stack

<p align="left">
  <img src="https://skillicons.dev/icons?i=go" alt="Go" height="50" />
  <img src="https://skillicons.dev/icons?i=postgres" alt="PostgreSQL" height="50" />
  <img src="https://skillicons.dev/icons?i=docker" alt="Docker" height="50" />
</p>

for non-cached system you can use this command:
go test -v ./src/tests/integration

## Documentation

- Go doc (terminal):

  - View all exported APIs: `cd src; go doc -all`
  - View a package: `go doc ./src/models`
  - View a symbol: `go doc ./src/models Post`

- Swagger/OpenAPI UI:

  - Spec file: `docs/openapi.yaml`
  - Serve static docs locally: `go run ./cmd/docserver`
  - Open in browser: `http://localhost:8090`

- Optional: Install Swagger generator (no code changes required here):
  - `go install github.com/swaggo/swag/cmd/swag@latest`
