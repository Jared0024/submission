FROM rust:1.93.1-slim-bookworm AS rust-build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY dispatch.c ./
RUN mkdir -p /artifacts && \
    cc -O2 -s dispatch.c -o /artifacts/dispatch && \
    cargo build --release --locked && \
    install -s target/release/session-pipeline /artifacts/portable && \
    if [ "$(uname -m)" = x86_64 ]; then \
        cargo rustc --release --locked --bin session-pipeline -- -C target-cpu=x86-64-v3 && \
        install -s target/release/session-pipeline /artifacts/v3; \
    else \
        ln /artifacts/portable /artifacts/v3; \
    fi

FROM debian:bookworm-slim AS runtime
COPY --from=rust-build /artifacts/dispatch /usr/local/bin/session-pipeline
COPY --from=rust-build /artifacts/portable /artifacts/v3 /usr/local/lib/session-runtime/
USER 1000:1000
ENTRYPOINT ["/usr/local/bin/session-pipeline"]
