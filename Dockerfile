FROM rust:1.93.1-slim-bookworm AS rust-build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked && strip target/release/session-pipeline

FROM debian:bookworm-slim AS runtime
COPY --from=rust-build /src/target/release/session-pipeline /usr/local/bin/session-pipeline
USER 1000:1000
ENTRYPOINT ["/usr/local/bin/session-pipeline"]
