# Changelog

## [0.4.0](https://github.com/non7top/claude-chatter/compare/v0.3.0...v0.4.0) (2026-11-14)


### Features

* **client:** let a bridge be subscribed to and publish its idle notice ([68edeea](https://github.com/non7top/claude-chatter/commit/68edeea18e2c698e4b82318994556b7ee81b9b81))
* **client:** let a bridge be subscribed to and publish its idle notice ([f18bf44](https://github.com/non7top/claude-chatter/commit/f18bf445ce8fc9e3a5c2bb6df28191e16c3530ee))

## [0.3.0](https://github.com/non7top/claude-chatter/compare/v0.2.2...v0.3.0) (2026-11-14)


### Features

* **client:** subscribe to another session going idle with notify_when_idle ([354f148](https://github.com/non7top/claude-chatter/commit/354f1484be50c7b5bd6c269a3f65837252276853))
* **client:** subscribe to another session going idle with notify_when_idle ([bfdb902](https://github.com/non7top/claude-chatter/commit/bfdb9023ae9f9d431da28b0207cf0c2db7bc34a3))
* **send:** open outbound connections with the target's auth line ([160b6a2](https://github.com/non7top/claude-chatter/commit/160b6a2201dec67926c8d7c14ab78a014d469351))


### Bug Fixes

* **client:** keep the inbound copy of a message a session sends to itself ([61db555](https://github.com/non7top/claude-chatter/commit/61db5555ac9e38203d8b52494be57571ac1f443f))
* **client:** keep the inbound copy of a message a session sends to itself ([9391ca2](https://github.com/non7top/claude-chatter/commit/9391ca25ec0fa705cb675f12085726c1dcb128a3))
* **discovery:** name the key file by the socket path hash like Claude Code does ([6311c61](https://github.com/non7top/claude-chatter/commit/6311c6148f6d5fb54bb44098487a21d0db12426b))
* **send:** refuse a message over the frame limit instead of sending it ([59b1edd](https://github.com/non7top/claude-chatter/commit/59b1eddc80fc5eba2c3389c4c29ceebe6ae5b4e9))
* **send:** refuse a message over the frame limit instead of sending it ([1475516](https://github.com/non7top/claude-chatter/commit/1475516f02d49e1d636782e7439b730f05a48605))
* **storage:** close SQLite connections that were left to the garbage collector ([6c7ca38](https://github.com/non7top/claude-chatter/commit/6c7ca38ddc1567cfce1f25ca40aad4ac4e609a83))

## [0.2.2](https://github.com/non7top/claude-chatter/compare/v0.2.1...v0.2.2) (2026-11-13)


### Bug Fixes

* **discovery:** keep sessions that share a name and refuse ambiguous sends ([a560d7b](https://github.com/non7top/claude-chatter/commit/a560d7b994d10974248d4158fd4d230550eaeb87))
* **discovery:** keep sessions that share a name and refuse ambiguous sends ([4255fb4](https://github.com/non7top/claude-chatter/commit/4255fb44bf3b83dcd3fb07bf90d52e700275b8bb))

## [0.2.1](https://github.com/non7top/claude-chatter/compare/v0.2.0...v0.2.1) (2026-11-13)


### Bug Fixes

* **packaging:** prepare PyPI release (metadata, README tools, publish workflow) ([fee34ef](https://github.com/non7top/claude-chatter/commit/fee34ef8bc46c145a6485b8d98a11e44773948f5))

## [0.2.0](https://github.com/non7top/claude-chatter/compare/v0.1.0...v0.2.0) (2026-11-11)


### Features

* add a persistent log file alongside stderr ([fddd312](https://github.com/non7top/claude-chatter/commit/fddd3124110ced017b9939f57ca028fe3be36321))
* add list_jobs and --jobs, drop peer tokens from list_sessions output ([ca03f34](https://github.com/non7top/claude-chatter/commit/ca03f347293723336f9c96a6374dec87fd2f8ecb))
* add read_messages tool with unread/read tracking (inbox model) ([1823dfc](https://github.com/non7top/claude-chatter/commit/1823dfc6ccb22c22c58fb56097585c84afd8a827))
* **cli:** add standalone daemon and CLI execution modes (--standalone, --list, --send, --purge) ([98bfae4](https://github.com/non7top/claude-chatter/commit/98bfae44d5af5cba0ac11ba35112df6b1a981a8a))
* core peer messaging protocol and client library ([2a9216f](https://github.com/non7top/claude-chatter/commit/2a9216f1065fb8e83e5eaca4ed73cc8c3d36fa40))
* core peer messaging protocol and client library ([1a1e8c9](https://github.com/non7top/claude-chatter/commit/1a1e8c98f91bc85b3a662e84d4248fb0cbf30d85))
* default session name is bridge-&lt;cwd basename&gt;, with generic CHATTER_* env vars ([dfd55ef](https://github.com/non7top/claude-chatter/commit/dfd55ef811b8323cba82b4ccfb354941ecfa4ba5))
* **inbox:** persist messages and read state across restarts using SQLite ([43fc8d4](https://github.com/non7top/claude-chatter/commit/43fc8d45e10c64d22d06b49ec87b4a9191fc01da))
* migrate MCP layer to FastMCP, rename project to claude-chatter-mcp, add MIT license ([3c84338](https://github.com/non7top/claude-chatter/commit/3c84338de0f8a80709ed09575972f7ffbe43e2ed))
* note async-mailbox semantics in the inbound ACK ([91ad7a5](https://github.com/non7top/claude-chatter/commit/91ad7a5d613333bd8bb4daa2d424bed49fffc959))
* nudge agy to prime push via read_messages hint, and stop it polling in a loop ([979df9a](https://github.com/non7top/claude-chatter/commit/979df9ad1aff6f8503feab3fb5935c430e5fee19))
* optional push into an agy conversation via a one-time --prime from its shell ([87c003b](https://github.com/non7top/claude-chatter/commit/87c003bfb1880dc69d98c452d5fdcc4179cef81b))
* package the repo as an Antigravity plugin with a PreInvocation prime hook ([7fe6b0d](https://github.com/non7top/claude-chatter/commit/7fe6b0db21bb60a25b198035c039c2617fff50fb))
* persist a renamed session's identity across restarts ([0675771](https://github.com/non7top/claude-chatter/commit/067577196f43606b1f4099fdef29dc9c88bb8689))
* prime_push tells the agent to run the returned command ([2425a88](https://github.com/non7top/claude-chatter/commit/2425a883cc15b94ec252f038a6c27c14afb9b5d2))
* run the plugin via uvx, add chatter skill, rename prime_push to enable_push, prime via ephemeralMessage ([5b2b6cc](https://github.com/non7top/claude-chatter/commit/5b2b6cc3cacb8f7e466fd5cb133d3098017b392a))
* **session:** add rename_session tool call for dynamic bridge session renaming ([9b76f8f](https://github.com/non7top/claude-chatter/commit/9b76f8f303a390c09736ca7694da0c196385ee7f))
* **standalone:** add auto-reply pong turns for standalone bridge daemons and fix socket ACK connection reset handling ([9e30306](https://github.com/non7top/claude-chatter/commit/9e30306ce780e9aaf99e2fe1b6e93c23d2fd1464))
* **storage:** track stable session identity across renames and handle edge cases ([394ae71](https://github.com/non7top/claude-chatter/commit/394ae71e9778d20444d2dac0fb9a91e740d52440))
* **transcript:** enhance transcript file resolution with cwd prediction and extended creation polling ([6e9953d](https://github.com/non7top/claude-chatter/commit/6e9953d0ac1c9da603588282ccf51e61eb268b80))


### Bug Fixes

* **cli:** eliminate side effects in read-only modes and skip reload wait ([3ac8fab](https://github.com/non7top/claude-chatter/commit/3ac8fab63002375a7947b420793aec4741633685))
* delegate rename_session to the owning persistent daemon ([1896806](https://github.com/non7top/claude-chatter/commit/1896806fae6b8fdb2919757faff948c7929c486d))
* don't treat the uv-run venv bin as on the agent shell's PATH ([1a4ddd8](https://github.com/non7top/claude-chatter/commit/1a4ddd85c9ba48928d3ebcfa6c1fa76f098e511c))
* enable_push accepts and ignores a session_id argument ([9af028a](https://github.com/non7top/claude-chatter/commit/9af028a4e7ece7e9aa5b8b00cd564a2f6d8a3e45))
* give the prime command a stable path so it can be allow-listed ([955daa0](https://github.com/non7top/claude-chatter/commit/955daa05cf6d2eacc9dbcef9b8867428e6e2bf7d))
* handle SIGTERM for a clean exit in the default MCP stdio mode ([7d26596](https://github.com/non7top/claude-chatter/commit/7d2659623c1abf7774bf53f6099fdb3a47f966d4))
* **inbox:** composite primary key, bare cli resolution and consistent ordering ([8165043](https://github.com/non7top/claude-chatter/commit/8165043f3c809f19afce4395e427e3d5c8d65aa9))
* make the agy-side bridge daemon persistent across MCP host restarts ([f16cf56](https://github.com/non7top/claude-chatter/commit/f16cf56d1087e216692c18c3517c1e6f639900a3))
* map other hosts' parameter names onto tool arguments and drop unknown ones ([3295b8a](https://github.com/non7top/claude-chatter/commit/3295b8a07cfa05b6f666c41ed5d980909963b4f3))
* match the real Claude Code cross-session wire format on send ([57c233e](https://github.com/non7top/claude-chatter/commit/57c233e10ad183092b524fb5b5a5ae2134e53f18))
* persist a random-suffixed session name per project path, resolved from the agy process ([1fa2aae](https://github.com/non7top/claude-chatter/commit/1fa2aaed9cbf92e76ab9857535593664cf572cab))
* **protocol:** allocate fresh session id for concurrent clashing sessions ([ae60136](https://github.com/non7top/claude-chatter/commit/ae6013601b31e7da5fe952b0f3938a772c46743a))
* register real machine-id/PID-namespace/procStart in session descriptors ([cda59dc](https://github.com/non7top/claude-chatter/commit/cda59dc72e78cc5fd2c7e4cf9aeefa947f98b968))
* remove transcript-scraping response extraction, bump Docker base to 3.14 ([fde254b](https://github.com/non7top/claude-chatter/commit/fde254b79fb61a85a03582eff9e82fbb201269aa))
* shorter prime command, and find agy's executable after a self-update ([e22aace](https://github.com/non7top/claude-chatter/commit/e22aacec54aaa5876a5fe413fa4c37938c5f112f))
* stop acking inbound frames so Claude Code's SendMessage reports success ([0d49cb0](https://github.com/non7top/claude-chatter/commit/0d49cb082078cc50e4d90fa35e17ce1550e575c5))
* stop saying nothing can wake the host now that push exists ([473bd4f](https://github.com/non7top/claude-chatter/commit/473bd4f60879203f78b9c97c1731cbc8949d7c8e))
* **storage:** add read-only store mode and resolve live bridge descriptors ([e54ee3e](https://github.com/non7top/claude-chatter/commit/e54ee3e1ce0b17f119420bb49d9cf69562a52e80))
* **storage:** address peer review items A-F on session identity and migration ([7f703a4](https://github.com/non7top/claude-chatter/commit/7f703a42464360c428c6b54a8c1de43f1a2be97f))
* **storage:** test real WAL DB for read-only inspect and document empty sidecars ([039183e](https://github.com/non7top/claude-chatter/commit/039183eb54ffe4fa324e3aac216401cf2300d620))
* strip the hardcoded auto-reply Pong scaffold ([44a1a1b](https://github.com/non7top/claude-chatter/commit/44a1a1b49a670d2cd56e55b23a1f61022ba1aa93))


### Reverts

* remove auto-spawned persistent daemon architecture ([f652306](https://github.com/non7top/claude-chatter/commit/f652306a4eec16508189ee4ff7ed575d63856da3))
