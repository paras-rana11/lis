dont use emojis like
🚫 Avoid These in .log Files:

| Emoji     | Unicode  | Problem                   |
| ------    | -------- | ------------------------- |
| ✅ (✅)  | `\u2705` | Emoji – can break logging |
| 🟢, 🔴   | Various  | Emoji – same problem      |



use symbols is allowed with utf-8:

| Symbol   | Description          | Unicode  | Safe in `.log` files? |
| ------   | -------------------- | -------- | --------------------- |
| ✔ (✓)   | Check mark           | `\u2714` | ✅ Yes                |
| ❌      | Cross mark (failure) | `\u274C` | ✅ Yes                |
| ⚠️      | Warning sign         | `\u26A0` | ✅ Yes                |
