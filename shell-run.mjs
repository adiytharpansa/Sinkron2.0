// Runner invoked by an external scheduler. Usage: node shell-run.mjs <fast|full> [--dry-run]
// Environment passthrough: SHARD_INDEX, CONCURRENCY, SYNC_MAX (optional).
// CFG_1/CFG_2/CFG_3 (optional) are mapped onto the names the sync expects.
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import path from "node:path";

const root = path.dirname(fileURLToPath(import.meta.url));
const mode = process.argv[2] === "full" ? "full" : "fast";
const dry = process.argv.includes("--dry-run");

const env = { ...process.env };
if (env.CFG_1) env.SUPABASE_URL = env.CFG_1;
if (env.CFG_2) env.SUPABASE_SERVICE_ROLE_KEY = env.CFG_2;
if (env.CFG_3) env.REVALIDATE_SECRET = env.CFG_3;
env.SYNC_MODE = mode;
env.SHARD_TOTAL = env.SHARD_TOTAL || "4";
env.CONCURRENCY = env.CONCURRENCY || "5";
env.SYNC_MAX = env.SYNC_MAX || (mode === "full" ? "60" : "20");

function run(cmd, args) {
  const r = spawnSync(cmd, args, { cwd: root, stdio: "inherit", env });
  if (r.error) { console.error("spawn failed:", r.error.message); process.exit(1); }
  if (r.status !== 0) process.exit(r.status == null ? 1 : r.status);
}

run("npm", ["ci", "--no-audit", "--no-fund"]);
const args = ["scripts/sync.mjs"];
if (dry) args.push("--dry-run");
run("node", args);
