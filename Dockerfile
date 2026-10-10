# UNTESTED: written without a container runtime available. Expect to adjust it.
# See docs/deployment.md for the sandbox trade-off.

FROM rust:1.88-slim AS build
WORKDIR /src
RUN apt-get update && apt-get install -y --no-install-recommends pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends chromium ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 crawler
COPY --from=build /src/target/release/mcpcrawler /usr/local/bin/mcpcrawler
USER crawler
ENV CHROME=/usr/bin/chromium
# Chrome's sandbox needs user namespaces. If the container runtime does not
# provide them, set MCPCRAWLER_BROWSER_NO_SANDBOX=1 and accept what that costs.
ENTRYPOINT ["/usr/local/bin/mcpcrawler"]
