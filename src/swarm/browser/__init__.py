"""Browser runtime (adapters layer): Chrome over CDP, plus the action guard that gates requests.

Facade: `BrowserRuntime.open_session` (the Protocol lives in `ports`; `chrome.ChromeRuntime`
implements it). May import shared, store, and nothing else of swarm.
It must never know which engine or harness drives a session, so it never imports `engine` or
`harness`; the harness reaches only `browser.cdp`, and the daemon wires the rest.
"""
