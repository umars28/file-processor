FROM golang:1.27.3-alpine AS build
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/fileproc ./cmd/fileproc

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/fileproc /fileproc
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/fileproc"]
CMD ["serve"]
