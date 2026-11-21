---
name: swarm-front
description: Forward every human Telegram message to swarmd (the always-on browser employee) through the swarm MCP server's swarm_tell tool, verbatim, and relay its ack to the same topic. Use for every message that arrives from Telegram.
---

# swarm-front

swarmd is the user's always-on browser employee on this machine. It owns the projects, the
work queue and the open questions. Hermes is only its front door on Telegram: Hermes never
decides what a message means, swarmd does. The tools come from the `swarm` MCP server
(`http://127.0.0.1:8765/mcp`, see `hermes/config-snippet.yaml`).

## The rule

Telegram에서 받은 사람 메시지는 **원문 그대로** `swarm_tell(channel, text)`로 넘긴다. 요약·번역·해석하지 않는다. channel은 `telegram:<chat_id>:<thread_id>`(토픽 없으면 `telegram:<chat_id>`). 반환된 `ack`를 같은 토픽에 그대로 보낸다. 상태 질문이라도 직접 답하지 말고 swarm_tell로 넘긴다.

This also covers answers to swarmd's questions ("#2 B로 가"), feedback, "멈춰" and
"요금제 페이지만 봐". All of them go to `swarm_tell`.

### swarm_tell

- Call: `swarm_tell(channel: str, text: str)`.
- Returns `{ack, project_id, order_id, closed_ask_id}`. Send `ack` to the same chat and topic
  the message came from, unchanged. The ids are for logs only. Don't show them.
- Within 10 s it always returns an ack. When the ack says "받았어요, 정리해서 답할게요",
  swarmd sends the real answer to the topic by itself later. Don't add anything of your own.
- If the tool call fails (swarmd down, timeout), reply once in the same topic:
  "⚠️ swarmd가 응답하지 않아요. 잠시 후 다시 보내 주세요." Don't retry, and don't answer the
  message yourself.

## Other tools: Hermes housekeeping and display only

The tools below never answer a human message. A human's question, even "지금 뭐 해?",
goes to `swarm_tell` under the rule above. Use these only to check the setup or to display
swarmd's state where Hermes itself shows it (for example a dashboard view). Never use them to
send anything to Telegram, and never call them on a schedule. swarmd owns every outbound
message (quiet hours, dedupe). If anything here seems to conflict with the rule, the rule wins.

### swarm_projects

- Call: `swarm_projects()`, no arguments.
- Returns a list of `{id, slug, name, channel, objective, open_asks}`.
- Use it to know which Telegram topics belong to swarm projects (`channel`), for example to
  check after setup that a topic's `telegram:<chat_id>:<thread_id>` is mapped. A message from
  a topic that is not in this list still goes to `swarm_tell`. swarmd answers it in General.

### swarm_status

- Call: `swarm_status(channel: str)`, with the same channel format as `swarm_tell`.
- Returns a plain-text status of the project mapped to `channel`, or of every project for an
  unmapped channel. At most 3,500 characters.
- Use it only for a status block that Hermes itself shows, such as a dashboard widget. When a
  human asks for status in Telegram, use `swarm_tell`.

### swarm_inbox

- Call: `swarm_inbox()`, no arguments.
- Returns the open questions waiting for a human:
  `{ask_id, project_id, run_id, kind, question, options, recommended, irreversible, deadline,
  number}`. `number` is what the human answers with ("#2 B로 가").
- swarmd already sends every question to Telegram itself. Never re-send them, and never
  answer one for the human. This is for display only.

### swarm_report

- Call: `swarm_report(channel: str, kind: "daily" | "weekly")`.
- Returns the report text, for display where Hermes itself needs to show one. If it takes
  longer than about 8 s, it returns "📊 보고서를 만드는 중이에요…" and swarmd posts the report to
  the topic itself, so don't call it again.
- Reports belong to swarmd. It sends the 08:00 daily briefing and the Friday 17:00 weekly
  report itself. Never schedule, push or forward reports from Hermes. A human asking for a
  report ("보고서 보여줘") goes to `swarm_tell`, like every other human message.

## Kanban

swarmd mirrors its projects onto the Hermes kanban, one board per project slug (for example
`compet`). Its cards have no assignee. Never assign, dispatch or run them with Hermes agents.
`kanban.dispatch_profiles` excludes those boards (see `hermes/config-snippet.yaml`).
