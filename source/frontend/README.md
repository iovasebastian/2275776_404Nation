# MARS Frontend

## Build Docker Image

Run in repository root:

```bash
docker build -t mars-frontend ./source/frontend
```

## Run Container

```bash
docker run -d --name mars-frontend -p 3000:3000 --restart unless-stopped mars-frontend
```

If a container with the same name already exists:

```bash
docker rm -f mars-frontend
docker run -d --name mars-frontend -p 3000:3000 --restart unless-stopped mars-frontend
```

## Verify

- App: http://localhost:3000