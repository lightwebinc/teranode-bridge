// Command ci is the Dagger-backed CI/CD pipeline for teranode-bridge.
//
// Usage:
//
//	go run ./ci <subcommand> [flags]
//
// Subcommands:
//
//	unit       go test -race ./...
//	lint       go vet + golangci-lint
//	vuln       govulncheck
//	tidy       go mod tidy diff check
//	build      go build ./...
//	image      build OCI image (and optionally export/publish)
//	all        tidy + lint + vuln + unit + build + image
//	dev-shell  interactive shell in the builder container
//
// Flags:
//
//	-src     path to repo source (default ".")
//	-common  path to shard-common sibling (default "../shard-common")
//	-version version ldflag value (default "dev")
//	-address registry ref for `image` publish (e.g. ghcr.io/foo/bar:tag)
//	-export  tarball path for `image` export
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"dagger.io/dagger"
)

const (
	repoName     = "teranode-bridge"
	commonModule = "github.com/lightwebinc/shard-common"
	goImage      = "golang:1.27-alpine"
	lintImage    = "golangci/golangci-lint:v2.13.2-alpine"
)

// buildTargets are the package paths to `go build` in the build step. The
// Dockerfile is the source of truth for the runtime image; this is only a
// source-level compile check.
var buildTargets = []string{"./..."}

func main() {
	var (
		src     = flag.String("src", ".", "path to repo source")
		common  = flag.String("common", "../shard-common", "path to shard-common sibling")
		version = flag.String("version", "dev", "version ldflag value")
		address = flag.String("address", "", "registry ref for image publish")
		export  = flag.String("export", "", "tarball path for image export")
	)
	flag.Parse()

	cmd := flag.Arg(0)
	if cmd == "" {
		cmd = "all"
	}

	ctx := context.Background()
	c, err := dagger.Connect(ctx, dagger.WithLogOutput(os.Stderr))
	if err != nil {
		log.Fatalf("dagger connect: %v", err)
	}
	defer c.Close()

	p := &pipeline{
		c:       c,
		src:     *src,
		common:  *common,
		version: *version,
	}

	switch cmd {
	case "unit":
		fail(p.unit(ctx))
	case "lint":
		fail(p.lint(ctx))
	case "vuln":
		fail(p.vuln(ctx))
	case "tidy":
		fail(p.tidy(ctx))
	case "build":
		fail(p.build(ctx))
	case "image":
		fail(p.image(ctx, *address, *export))
	case "all":
		for _, step := range []struct {
			name string
			run  func(context.Context) error
		}{
			{"tidy", p.tidy},
			{"lint", p.lint},
			{"vuln", p.vuln},
			{"unit", p.unit},
			{"build", p.build},
		} {
			fmt.Fprintf(os.Stderr, "==> %s\n", step.name)
			fail(step.run(ctx))
		}
		fmt.Fprintln(os.Stderr, "==> image")
		fail(p.image(ctx, "", ""))
	case "dev-shell":
		fail(p.devShell(ctx))
	default:
		log.Fatalf("unknown subcommand %q (try: unit lint vuln tidy build image all dev-shell)", cmd)
	}
}

func fail(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

// pipeline holds the Dagger client and pipeline-wide settings.
type pipeline struct {
	c       *dagger.Client
	src     string
	common  string
	version string
}

func (p *pipeline) repoSrc() *dagger.Directory {
	return p.c.Host().Directory(p.src, dagger.HostDirectoryOpts{
		Exclude: []string{".git", "build", "ci/build", "*.tar", "teranode-bridge"},
	})
}

func (p *pipeline) commonSrc() *dagger.Directory {
	return p.c.Host().Directory(p.common, dagger.HostDirectoryOpts{
		Exclude: []string{".git", "build"},
	})
}

func (p *pipeline) modCache() *dagger.CacheVolume   { return p.c.CacheVolume("go-mod-" + repoName) }
func (p *pipeline) buildCache() *dagger.CacheVolume { return p.c.CacheVolume("go-build-" + repoName) }
func (p *pipeline) lintCache() *dagger.CacheVolume  { return p.c.CacheVolume("golangci-" + repoName) }

// goBase returns a configured golang container with the repo mounted at /src,
// shard-common mounted at /common, the local replace directive applied, and
// modules downloaded.
//
// The replace is what lets a shard-common change be validated against this
// repo before it is tagged: CI checks out the matching shard-common branch when
// one exists, and main otherwise.
func (p *pipeline) goBase() *dagger.Container {
	return p.c.Container().From(goImage).
		WithEnvVariable("CGO_ENABLED", "0").
		WithEnvVariable("GOFLAGS", "-buildvcs=false").
		WithMountedCache("/go/pkg/mod", p.modCache()).
		WithMountedCache("/root/.cache/go-build", p.buildCache()).
		WithExec([]string{"apk", "add", "--no-cache", "git", "ca-certificates"}).
		WithDirectory("/src", p.repoSrc()).
		WithDirectory("/common", p.commonSrc()).
		WithWorkdir("/src").
		WithExec([]string{"go", "mod", "edit", "-replace", commonModule + "=/common"}).
		WithExec([]string{"go", "mod", "download"})
}

func (p *pipeline) unit(ctx context.Context) error {
	_, err := p.goBase().
		WithEnvVariable("CGO_ENABLED", "1").
		WithExec([]string{"apk", "add", "--no-cache", "gcc", "musl-dev"}).
		WithExec([]string{"go", "test", "-race", "-count=1", "./..."}).
		Sync(ctx)
	return err
}

func (p *pipeline) lint(ctx context.Context) error {
	if _, err := p.goBase().WithExec([]string{"go", "vet", "./..."}).Sync(ctx); err != nil {
		return err
	}
	_, err := p.c.Container().From(lintImage).
		WithMountedCache("/go/pkg/mod", p.modCache()).
		WithMountedCache("/root/.cache/go-build", p.buildCache()).
		WithMountedCache("/root/.cache/golangci-lint", p.lintCache()).
		WithDirectory("/src", p.repoSrc()).
		WithDirectory("/common", p.commonSrc()).
		WithWorkdir("/src").
		WithExec([]string{"go", "mod", "edit", "-replace", commonModule + "=/common"}).
		WithExec([]string{"golangci-lint", "run", "--timeout=5m", "./..."}).
		Sync(ctx)
	return err
}

func (p *pipeline) vuln(ctx context.Context) error {
	// Pinned: govulncheck@latest (>=v1.2, via x/tools v0.46.0) panics on Go 1.26
	// with "ForEachElement called on type containing *types.TypeParam" — a tooling
	// crash in its SSA analysis, not a vuln. v1.1.4 scans this module cleanly.
	_, err := p.goBase().
		WithExec([]string{"go", "install", "golang.org/x/vuln/cmd/govulncheck@v1.1.4"}).
		WithExec([]string{"sh", "-c", "/go/bin/govulncheck ./..."}).
		Sync(ctx)
	return err
}

// tidy verifies that `go mod tidy` produces no diff against the committed
// go.mod (after dropping the local replace directive). go.sum is intentionally
// not diffed: a local-path replace for shard-common legitimately pulls
// transitive-dep hashes into go.sum that are not in the committed file.
func (p *pipeline) tidy(ctx context.Context) error {
	_, err := p.c.Container().From(goImage).
		WithMountedCache("/go/pkg/mod", p.modCache()).
		WithDirectory("/src", p.repoSrc()).
		WithDirectory("/common", p.commonSrc()).
		WithWorkdir("/src").
		WithExec([]string{"apk", "add", "--no-cache", "git", "diffutils"}).
		WithExec([]string{"sh", "-c", "cp go.mod go.mod.orig"}).
		WithExec([]string{"go", "mod", "edit", "-replace", commonModule + "=/common"}).
		WithExec([]string{"go", "mod", "tidy"}).
		WithExec([]string{"go", "mod", "edit", "-dropreplace", commonModule}).
		WithExec([]string{"sh", "-c", "diff -u go.mod.orig go.mod"}).
		Sync(ctx)
	return err
}

func (p *pipeline) build(ctx context.Context) error {
	args := append([]string{"go", "build", "-buildvcs=false"}, buildTargets...)
	_, err := p.goBase().WithExec(args).Sync(ctx)
	return err
}

// image builds the runtime OCI image via the canonical Dockerfile, then
// optionally exports it as a tarball or publishes it to a registry.
//
// The image build resolves shard-common from the module proxy at its committed
// version — it does not use the sibling replace, so a published image never
// depends on an untagged working tree.
func (p *pipeline) image(ctx context.Context, address, exportPath string) error {
	img := p.repoSrc().DockerBuild(dagger.DirectoryDockerBuildOpts{
		Dockerfile: "Dockerfile",
		BuildArgs:  []dagger.BuildArg{{Name: "VERSION", Value: p.version}},
	})

	if address != "" {
		ref, err := img.Publish(ctx, address)
		if err != nil {
			return fmt.Errorf("publish: %w", err)
		}
		fmt.Println("published:", ref)
		return nil
	}

	if exportPath != "" {
		if err := os.MkdirAll(filepath.Dir(exportPath), 0o755); err != nil {
			return err
		}
		if _, err := img.AsTarball().Export(ctx, exportPath); err != nil {
			return fmt.Errorf("export: %w", err)
		}
		fmt.Println("exported:", exportPath)
		return nil
	}

	_, err := img.Sync(ctx)
	return err
}

func (p *pipeline) devShell(ctx context.Context) error {
	_, err := p.goBase().
		WithExec([]string{"apk", "add", "--no-cache", "bash"}).
		Terminal(dagger.ContainerTerminalOpts{Cmd: []string{"bash"}}).
		Sync(ctx)
	return err
}
