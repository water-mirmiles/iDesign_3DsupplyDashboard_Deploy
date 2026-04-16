#!/usr/bin/env node
/**
 * git-notify.js
 *
 * 目标：在本地 commit 完成后，用“按钮式交互”提示用户是否 push。
 *
 * 说明：
 * - 纯 Node 无法直接调用 VS Code 的 `window.showInformationMessage`（需要扩展 API）。
 * - 这里优先尝试 macOS 的 AppleScript 弹窗（带按钮），否则降级为输出可 Run 的 bash 代码块。
 */

import { execSync } from 'node:child_process';

function sh(cmd) {
  return execSync(cmd, { stdio: ['ignore', 'pipe', 'pipe'] }).toString('utf8').trim();
}

function escapeAppleScriptString(s) {
  return String(s).replace(/\\/g, '\\\\').replace(/"/g, '\\"');
}

const arg = process.argv[2];
let sha = arg && arg !== '-' ? String(arg) : '';
if (!sha) {
  try {
    sha = sh('git rev-parse --short HEAD');
  } catch {
    sha = 'UNKNOWN_SHA';
  }
}

const pushCmd = 'git push origin main';

// macOS：弹窗带按钮（最接近“UI 按钮交互”）
try {
  const title = `✅ 本地提交已完成 (${sha})`;
  const msg = '请选择下一步操作：';
  const osa = [
    'osascript',
    '-e',
    `"display dialog \\"${escapeAppleScriptString(msg)}\\" with title \\"${escapeAppleScriptString(title)}\\" buttons {\\"暂时仅保留本地\\", \\"立即同步云端\\"} default button \\"暂时仅保留本地\\""`,
  ].join(' ');

  const out = sh(osa);
  const chosePush = /button returned:立即同步云端/.test(out);
  if (chosePush) {
    process.stdout.write(`[git-notify] 选择：立即同步云端\\n`);
    process.stdout.write(`[git-notify] 执行：${pushCmd}\\n`);
    execSync(pushCmd, { stdio: 'inherit' });
  } else {
    process.stdout.write(`[git-notify] 选择：暂时仅保留本地\\n`);
  }
  process.exit(0);
} catch {
  // ignore and fallback
}

// 降级：输出可点击 Run 的代码块（在 Cursor/VS Code 终端中更可靠）
process.stdout.write('---\n');
process.stdout.write(`✅ 本地提交已完成 (${sha})\n`);
process.stdout.write('如需同步到 GitHub，请点击下方命令的 [Run]：\n\n');
process.stdout.write('```bash\n');
process.stdout.write(`${pushCmd}\n`);
process.stdout.write('```\n');

