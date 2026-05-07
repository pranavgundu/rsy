#!/usr/bin/env node
"use strict";
const { execFileSync } = require("child_process");
const path = require("path");
const fs = require("fs");

const bin = path.join(__dirname, "rsy");
if (!fs.existsSync(bin)) {
  console.error("rsy: binary not found. Try reinstalling: npm install -g @gundu/rsy");
  process.exit(1);
}
try {
  execFileSync(bin, process.argv.slice(2), { stdio: "inherit" });
} catch (e) {
  process.exit(e.status ?? 1);
}
