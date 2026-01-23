clean-deps:
	docker compose down

start-deps:
	@command -v docker >/dev/null 2>&1 && docker compose up -d || echo "Docker not found, skipping start-deps"

setup-db:
	@echo "Waiting for PostgreSQL and running migrations..."
	@for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30; do \
		cargo sqlx migrate run 2>/dev/null && echo "Migrations complete" && exit 0; \
		echo "Attempt $$i: Database not ready, waiting..."; \
		sleep 1; \
	done; \
	echo "Database failed to become ready after 30 attempts"; \
	cargo sqlx migrate run

reset-deps: clean-deps start-deps setup-db

test-in-ci: start-deps setup-db
	cargo nextest run --workspace --verbose
	cargo test --doc --workspace
	cargo doc --no-deps --workspace

check-code:
	SQLX_OFFLINE=true cargo fmt --check --all
	SQLX_OFFLINE=true cargo check --workspace
	SQLX_OFFLINE=true cargo clippy --workspace --all-features
	SQLX_OFFLINE=true cargo audit
	SQLX_OFFLINE=true cargo deny check

sqlx-prepare:
	cargo sqlx prepare --workspace
