NIX_DEPS_DIR := .nix-deps

# Seconds the fuzz target runs in `make fuzz`.
FUZZ_TIME := 60

.PHONY: start-deps clean-deps setup-db reset-deps sqlx-prepare check-code fuzz

start-deps:
	@mkdir -p $(NIX_DEPS_DIR)
	@set -e; \
	  eval "$$(nix run .#dev-env)"; \
	  nix run .#nix-deps-base -- up -D; \
	  for i in $$(seq 1 60); do \
	    if nix run .#nix-deps-base -- project is-ready 2>/dev/null; then break; fi; \
	    if [ "$$i" = "60" ]; then \
	      echo "ERROR: deps not ready after 5 minutes" >&2; \
	      nix run .#nix-deps-base -- process list || true; \
	      exit 1; \
	    fi; \
	    sleep 5; \
	  done; \
	  nix run .#setup-db-dev

clean-deps:
	-@eval "$$(nix run .#dev-env)"; nix run .#nix-deps-base -- down
	chmod -R u+w $(NIX_DEPS_DIR) 2>/dev/null || true
	rm -rf $(NIX_DEPS_DIR)

setup-db:
	nix run .#setup-db-dev

reset-deps: clean-deps start-deps

check-code:
	nix flake check

sqlx-prepare:
	cargo sqlx prepare --workspace -- --all-targets

# Coverage-guided fuzzing via the shared vendored script
# (ci/vendor/tasks/fuzz.sh, from galoy-concourse-shared), also used by
# `nix run .#fuzz` and the Concourse `fuzz` job. Auto-discovers targets via
# `cargo fuzz list`; runs them for $(FUZZ_TIME)s. Corpus in fuzz/corpus/ (gitignored).
fuzz:
	FUZZ_SECONDS=$(FUZZ_TIME) bash ci/vendor/tasks/fuzz.sh
