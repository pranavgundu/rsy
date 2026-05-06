#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");
const os = require("os");

const platform = os.platform(); // darwin, linux, win32
const arch = os.arch();         // x64, arm64

const archMap = { x64: "x64", arm64: "arm64", arm: "arm64" };
const mappedArch = archMap[arch];

if (!mappedArch) {
  console.error(`rsy: unsupported architecture: ${arch}`);
  process.exit(1);
}

const pkgName = `@gundu/rsy-${platform}-${mappedArch}`;
const isWindows = platform === "win32";
const binaryName = isWindows ? "rsy.exe" : "rsy";

let pkgDir;
try {
  pkgDir = path.dirname(require.resolve(`${pkgName}/package.json`));
} catch {
  console.error(
    `rsy: could not find platform package "${pkgName}".\n` +
    `Install it manually: npm install ${pkgName}`
  );
  process.exit(1);
}

const src = path.join(pkgDir, "bin", binaryName);
const binDir = path.join(__dirname, "bin");
const dst = path.join(binDir, binaryName);

if (!fs.existsSync(src)) {
  console.error(`rsy: binary not found at ${src}`);
  process.exit(1);
}

fs.mkdirSync(binDir, { recursive: true });

// Copy instead of symlink so it survives npm dedupe
fs.copyFileSync(src, dst);
if (!isWindows) fs.chmodSync(dst, 0o755);

// On Windows also write a bin/rsy shim so "rsy" works without .exe
if (isWindows) {
  fs.writeFileSync(
    path.join(binDir, "rsy"),
    `#!/usr/bin/env sh\nexec "$(dirname "$0")/rsy.exe" "$@"\n`
  );
  fs.chmodSync(path.join(binDir, "rsy"), 0o755);
}

console.log(`rsy: installed ${pkgName} -> ${dst}`);
