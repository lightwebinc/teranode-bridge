#!/usr/bin/env python3
"""Generate LICENSE-THIRD-PARTY for a Go repository, from what it actually links.

Why this exists rather than a hand-written file: nearly every licence a Go
binary pulls in requires its own text to travel with a BINARY distribution, not
only with source. Apache-2.0 section 4 requires a copy of the licence be given
to recipients of the Work; MIT requires the notice "in all copies or
substantial portions"; BSD-3 requires binary redistributions to reproduce the
notice; Open BSV requires its text and an attribution line. Static linking
discards every one of those files, so an image built from a Go binary carries
the code and none of the notices.

A hand-written list goes stale silently the first time a dependency is added.
This reads `go list -deps`, so it describes the artefact rather than someone's
memory of it.

Handles both a Go module and an npm package. The Go side reads `go list -deps`,
so a module in go.mod that nothing imports is correctly absent and an INDIRECT
dependency that IS imported is correctly present. The npm side reads
`npm ls --omit=dev --all`, which is the tree that lands in a production image.

Usage:  gen-third-party-licenses.py <repo-dir> [--check]
        --check exits 1 if the file on disk differs from what would be written.
"""
import os
import subprocess
import sys

LICENSE_NAMES = ("LICENSE", "LICENSE.txt", "LICENSE.md", "LICENCE", "COPYING", "License")


def run(args, cwd):
    env = dict(os.environ, GOWORK="off")
    out = subprocess.run(args, cwd=cwd, capture_output=True, text=True, env=env)
    if out.returncode != 0:
        sys.exit(f"{' '.join(args)}: {out.stderr.strip()}")
    return out.stdout


def own_module(repo):
    return run(["go", "list", "-m"], repo).strip()


def linked_modules(repo):
    """Modules whose code is actually reachable from this repo's packages.

    `go list -deps` walks imports, so a module in go.mod that nothing imports is
    correctly absent: it is not in the binary and carries no obligation. An
    INDIRECT dependency that is imported IS present, which is why go.mod's
    direct/indirect split is the wrong question.
    """
    mine = own_module(repo)
    pkgs = run(["go", "list", "-deps", "-f", "{{if .Module}}{{.Module.Path}}{{end}}", "./..."], repo)
    mods = {p for p in pkgs.split() if p and p != mine and not p.startswith(mine + "/")}
    return sorted(mods)


def module_dir(repo, mod):
    """Where the module's source sits in the module cache.

    An empty Dir means the module is in the graph but not downloaded, which is
    the normal state on a fresh CI checkout before anything has fetched it. Ask
    for it rather than failing: a licence check that only passes on a warm cache
    is a licence check that gets disabled.
    """
    d = run(["go", "list", "-m", "-f", "{{.Dir}}", mod], repo).strip()
    if not d:
        run(["go", "mod", "download", mod], repo)
        d = run(["go", "list", "-m", "-f", "{{.Dir}}", mod], repo).strip()
    return d or None


def license_files(d):
    found = []
    for name in LICENSE_NAMES:
        p = os.path.join(d, name)
        if os.path.isfile(p):
            found.append(p)
    return found


def detect(text):
    head = "\n".join(text.splitlines()[:6]).lower()
    for needle, label in (
        ("open bsv license version 6", "Open BSV License Version 6"),
        ("open bsv license version 5", "Open BSV License Version 5"),
        ("apache license", "Apache License 2.0"),
        ("mit license", "MIT License"),
        ("isc license", "ISC License"),
        ("mozilla public license", "Mozilla Public License 2.0"),
    ):
        if needle in head:
            return label
    if "redistribution and use in source and binary forms" in text.lower():
        return "BSD License"
    if "permission is hereby granted, free of charge" in text.lower():
        return "MIT License"
    return "see text below"


def npm_packages(repo):
    """The production dependency tree, which is what a production image holds.

    `tsc` does not bundle: the built JavaScript resolves bare imports against
    node_modules at runtime, so every package here is physically present in the
    image and its licence travels only if it is written down.
    """
    import json as _json

    # NPM_LS_JSON lets a caller supply the tree from elsewhere, for the common
    # case where node lives in a container and this script runs on the host.
    supplied = os.environ.get("NPM_LS_JSON")
    if supplied:
        raw = open(supplied, encoding="utf-8").read()
    else:
        out = subprocess.run(
            ["npm", "ls", "--omit=dev", "--all", "--json"],
            cwd=repo, capture_output=True, text=True,
        )
        raw = out.stdout
    try:
        tree = _json.loads(raw)
    except ValueError:
        sys.exit("npm ls produced no JSON; run `npm ci` first, or set NPM_LS_JSON")
    seen = {}

    def walk(node):
        for name, info in (node.get("dependencies") or {}).items():
            key = (name, info.get("version") or "")
            if key in seen:
                continue
            seen[key] = True
            walk(info)

    walk(tree)
    return sorted(seen)


def npm_build(repo, repo_name):
    entries = []
    for name, version in npm_packages(repo):
        d = os.path.join(repo, "node_modules", *name.split("/"))
        if not os.path.isdir(d):
            sys.exit(f"{name}: not installed at {d}; run `npm ci` first")
        files = license_files(d)
        if not files:
            sys.exit(f"{name}: no licence file in {d}. Resolve by hand before shipping.")
        text = "\n\n".join(open(f, encoding="utf-8", errors="replace").read().rstrip() for f in files)
        entries.append((f"{name}@{version}", detect(text), text))
    return entries


def build(repo, repo_name):
    has_docker = os.path.exists(os.path.join(repo, "Dockerfile"))
    if os.path.exists(os.path.join(repo, "package.json")) and not os.path.exists(os.path.join(repo, "go.mod")):
        return render(repo_name, npm_build(repo, repo_name), "npm", has_docker)
    return render(repo_name, go_build(repo), "go", has_docker)


def go_build(repo):
    mods = linked_modules(repo)
    entries = []
    for m in mods:
        d = module_dir(repo, m)
        if not d:
            sys.exit(f"{m}: no module directory; run `go mod download` first")
        files = license_files(d)
        if not files:
            sys.exit(f"{m}: no licence file found in {d}. Resolve by hand before shipping.")
        text = "\n\n".join(open(f, encoding="utf-8", errors="replace").read().rstrip() for f in files)
        entries.append((m, detect(text), text))
    return entries


def render(repo_name, entries, kind, has_dockerfile=False):
    out = []
    out.append(f"Third-party licences for {repo_name}")
    out.append("=" * (len(repo_name) + 24))
    out.append("")
    out.append("GENERATED. Do not edit by hand.")
    out.append("")
    out.append("  scripts/gen-third-party-licenses.py <this repo>")
    out.append("")
    if kind == "go":
        out.append("Every module below is reachable from this repository's own packages, so")
        out.append("its code is inside any binary built here and inside any image carrying")
        out.append("one. Nearly all of these licences require their own text to travel with")
        out.append("a BINARY distribution and not only with source, and static linking")
        out.append("discards the files they live in. That is why this file exists.")
        if has_dockerfile:
            out.append("The Dockerfile copies it into the image.")
        else:
            # Asserting a Dockerfile that is not there is the same class of
            # error this file exists to prevent, one level up.
            out.append("Any release artefact or image built from this repository must")
            out.append("carry it, and this repository builds neither today.")
    else:
        out.append("Every package below is in the production dependency tree, so it is")
        out.append("physically present in any image built here: the compiler does not")
        out.append("bundle, and the built JavaScript resolves these imports at runtime.")
        out.append("Their own licence files do travel inside node_modules, which a Go")
        out.append("binary's do not, but this file states the set in one place and covers")
        out.append("any distribution that is not the whole tree.")
        if has_dockerfile:
            out.append("The Dockerfile copies it into the image.")
    out.append("")
    out.append("This repository's own source is Apache-2.0 and is in LICENSE, not here.")
    out.append("")
    out.append("Contents:")
    for i, (m, label, _) in enumerate(entries, 1):
        out.append(f"  {i:2d}. {m}  ({label})")
    out.append("")
    for i, (m, label, text) in enumerate(entries, 1):
        out.append("=" * 80)
        out.append(f"{i}. {m}")
        out.append(f"   {label}")
        out.append("=" * 80)
        out.append("")
        out.append(text)
        out.append("")
    return "\n".join(out).rstrip() + "\n"


def main():
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    repo = os.path.abspath(sys.argv[1])
    check = "--check" in sys.argv[2:]
    name = os.path.basename(repo)
    content = build(repo, name)
    target = os.path.join(repo, "LICENSE-THIRD-PARTY")
    if check:
        current = open(target, encoding="utf-8").read() if os.path.exists(target) else ""
        if current != content:
            sys.exit(f"{target} is stale: regenerate with scripts/gen-third-party-licenses.py")
        print(f"{target} is current")
        return
    with open(target, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"wrote {target} ({len(content.splitlines())} lines, {content.count(chr(61)*80)} licences)")


if __name__ == "__main__":
    main()
