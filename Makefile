# Makefile for iina-torrent-stream.
# Builds the TypeScript plugin and the native `torrentd` companion daemon.

PLUGIN_ID    := com.github.n0madic.iina-torrent-stream
PLUGIN_NAME  := iina-torrent-stream
IINA_PLUGINS := $(HOME)/Library/Application Support/com.colliderli.iina/plugins
DATA_DIR     := $(IINA_PLUGINS)/.data/$(PLUGIN_ID)
DEV_LINK     := $(IINA_PLUGINS)/$(PLUGIN_NAME).iinaplugin-dev

# VERSION is injected into the daemon's main.version via -ldflags -X. The
# release workflow exports VERSION from the git tag (e.g. v0.2.0); local
# builds default to "dev", which the plugin's ensureBinary() recognises
# and accepts without enforcing a version match.
VERSION      ?= dev
GO_LDFLAGS   := -s -w -X main.version=$(VERSION)

.PHONY: all plugin daemon daemon-release dev-daemon dev dev-uninstall package test clean help

help:
	@echo "Targets:"
	@echo "  plugin         Install npm deps and build the plugin into dist/"
	@echo "  daemon         Build a universal2 torrentd binary into build/ (local testing)"
	@echo "  daemon-release Build per-arch release binaries + print SHA-256 checksums"
	@echo "  dev-daemon     Build torrentd for this Mac and install it into the plugin data dir"
	@echo "  dev            Build the plugin and symlink it into IINA for development"
	@echo "  dev-uninstall  Remove the IINA dev-plugin symlink (does not touch the data dir)"
	@echo "  package        Build everything and create the .iinaplgz package"
	@echo "  test           Run the daemon's Go test suite"
	@echo "  clean          Remove all build artifacts"

all: plugin daemon

## Build the TypeScript plugin.
plugin:
	npm install
	npm run build

## Build a universal2 binary for local testing.
daemon:
	mkdir -p build
	cd torrentd && CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -trimpath -ldflags "$(GO_LDFLAGS)" -o ../build/torrentd-arm64 .
	cd torrentd && CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -trimpath -ldflags "$(GO_LDFLAGS)" -o ../build/torrentd-amd64 .
	lipo -create -output build/torrentd build/torrentd-arm64 build/torrentd-amd64
	rm build/torrentd-arm64 build/torrentd-amd64
	@echo "built build/torrentd (universal2)"

## Build per-arch release binaries to upload to a GitHub release.
## Asset names match `uname -m` so the plugin can download the right one.
daemon-release:
	mkdir -p build
	cd torrentd && CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -trimpath -ldflags "$(GO_LDFLAGS)" -o ../build/torrentd-darwin-arm64 .
	cd torrentd && CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -trimpath -ldflags "$(GO_LDFLAGS)" -o ../build/torrentd-darwin-x86_64 .
	codesign --force --sign - build/torrentd-darwin-arm64
	codesign --force --sign - build/torrentd-darwin-x86_64
	@echo "--- SHA-256 (copy into EXPECTED_SHA256 in src/daemon.ts) ---"
	@shasum -a 256 build/torrentd-darwin-arm64 build/torrentd-darwin-x86_64

## Build torrentd for this Mac and install it into the plugin data directory,
## so the development plugin runs without downloading anything.
dev-daemon:
	mkdir -p build
	cd torrentd && CGO_ENABLED=0 go build -trimpath -ldflags "$(GO_LDFLAGS)" -o ../build/torrentd-dev .
	mkdir -p "$(DATA_DIR)"
	cp build/torrentd-dev "$(DATA_DIR)/torrentd"
	chmod +x "$(DATA_DIR)/torrentd"
	-xattr -cr "$(DATA_DIR)/torrentd"
	@echo "installed torrentd into $(DATA_DIR)"

## Symlink the plugin into IINA for live development.
dev: plugin
	ln -sfn "$(CURDIR)" "$(DEV_LINK)"
	@echo "linked $(DEV_LINK)"
	@echo "Restart IINA, then enable the plugin under Settings > Plugins."

## Remove the IINA development symlink created by `make dev`. The plugin's
## data directory (downloaded torrentd binary, logs, state file) is left
## alone — wipe it manually with `rm -rf "$(DATA_DIR)"` if needed, after
## quitting IINA so the daemon is not holding files open.
dev-uninstall:
	@if [ -L "$(DEV_LINK)" ] || [ -e "$(DEV_LINK)" ]; then \
		rm -rf "$(DEV_LINK)"; \
		echo "removed $(DEV_LINK)"; \
	else \
		echo "nothing to remove (no symlink at $(DEV_LINK))"; \
	fi
	@echo "Restart IINA for the change to take effect."
	@echo "To also wipe the data dir (binary, logs, state):"
	@echo "  rm -rf \"$(DATA_DIR)\""

## Create the distributable .iinaplgz package.
package: plugin
	rm -rf build/pkg "$(PLUGIN_NAME).iinaplgz"
	mkdir -p build/pkg
	cp -R Info.json dist *.html README.md LICENSE NOTICE build/pkg/
	find build/pkg -name '*.map' -delete
	cd build/pkg && zip -r -X "../../$(PLUGIN_NAME).iinaplgz" . -x '.*'
	@echo "created $(PLUGIN_NAME).iinaplgz"

## Run the daemon's Go test suite.
test:
	cd torrentd && CGO_ENABLED=0 go test ./...

## Remove all build artifacts.
clean:
	rm -rf dist build .parcel-cache "$(PLUGIN_NAME).iinaplgz"
	@echo "cleaned"
