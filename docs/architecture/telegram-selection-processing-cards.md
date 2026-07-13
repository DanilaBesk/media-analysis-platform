# Telegram Selection And Processing Card Contract

Status: historical; superseded by the media-7f3.9.5 Telegram target materials and run-card flow
Beads: media-lgf
Date: 2026-05-16

## Purpose

This historical document defined the Telegram presentation contract before the target media_asset, selection_snapshot, analysis_run, and channel_surface vocabulary was implemented.

The product goal is to make Telegram feel like a task-oriented workspace:

- the user always has one current material selection to manage;
- each processing run is a separate task card with its own lifecycle;
- results belong to the processing task, not to the current selection;
- users can start a new processing run while older tasks are still running;
- the interface uses human product language, not internal technical terms.

## User-Facing Terms

Use these terms in Telegram copy:

- `Подборка` - the current mutable set of materials prepared for the next processing run.
- `Материалы` - the list of items inside the current set or a processing task snapshot.
- `Обработка` - a concrete processing task created from a snapshot of the current materials.
- `Результат` - the produced output artifact; for speech-to-text work this may be a transcript file.
- `Диагностика` - processing notes, warnings, and errors for a task.
- `Повторить` - start another processing run from the same task snapshot or copy its materials back into a new current set, depending on implementation stage.

Do not show these terms to users:

- `buffer`
- `selection`
- `analysis_run`
- `artifact`
- `job`
- `run`

These names may remain in internal code and API contracts.

## Core Model

Telegram has two visible card families.

### 1. Current Selection Card

Product name: `Подборка`.

This is the always-current control panel for materials the user is preparing now. It is mutable. It can be empty, have materials, show recent processing runs, and launch a new processing run.

Implementation names should align with the API contract, for example `current_materials_panel` or `current_materials_surface`. User copy must say `Подборка`.

### 2. Processing Task Card

Product name: `Обработка`.

Each task card represents one immutable snapshot and one API-owned `analysis_run`. It has its own status, buttons, result file, diagnostics, and retry path.

Task cards are independent from the current selection card. A running task must not block the user from building and launching a new current selection.

## Telegram Layout Principle

Telegram does not provide a real fixed-bottom panel. The application must emulate the "current selection is always at the bottom" behavior:

- after accepted user input, render or update the current selection card as the latest visible bot message;
- after starting a processing run, create a separate processing task card, clear the current selection, then render the current selection card again as the latest bot message;
- after task updates, edit the task card in place when possible and avoid stealing the bottom position from the current selection card unless the user explicitly opens that task;
- if the stored current selection card message cannot be edited, send a new current selection card and mark the old interaction surface as superseded.

## Card: Current Selection Empty

Text:

```text
Подборка
Материалов пока нет

Отправьте голосовое, аудио, видео, документ, ссылку или текст.
```

Buttons:

```text
[Последние]
[Обновить]
```

Rules:

- `Последние` is shown only when there are recent processing task cards.
- `Обновить` refreshes the current selection card from API state.
- No `Обработать` button is shown when there are no materials.

## Card: Current Selection With Materials

Text:

```text
Подборка
Материалов: 2

1. Голосовое из Telegram · 3.5 МБ
2. Документ · report.pdf

Последние обработки:
1. Готово · 2 материала · 14:25
2. В работе · 1 материал · 14:28
3. Ошибка · 4 материала · 14:31
```

Buttons:

```text
[Материалы]
[Очистить]
[Обработать (2)]
```

Optional recent-task navigation:

```text
[1] [2] [3]
```

Rules:

- `Обработать (N)` is the primary action and should be the last row.
- Do not use a microphone marker for the broad processing action; speech transcription is only one possible processing step.
- `Материалы` opens the detailed material-management screen.
- `Очистить` removes all current selection materials after confirmation or intentional friction if destructive-confirmation is added.
- Recent processing lines show at most 3 tasks.
- Recent tasks are navigation hints, not result actions for the current selection.

## Screen: Current Selection Materials

Text:

```text
Материалы
В подборке: 2

1. Голосовое из Telegram · 3.5 МБ
2. Документ · report.pdf
```

Buttons:

```text
[Убрать 1]
[Убрать 2]
[Очистить]
[К подборке]
```

Pagination buttons when needed:

```text
[Назад] [Дальше]
```

Rules:

- This screen manages only the current selection.
- Removing a material affects only current selection membership, not historical task snapshots.
- `К подборке` returns to the current selection card.

## Card: Processing Queued

Text:

```text
Обработка
Материалов: 2

Статус: в очереди
```

Buttons:

```text
[Отмена]
[Материалы]
[К подборке]
```

Rules:

- This card is created immediately after `Обработать`.
- The task snapshot is immutable.
- The current selection is cleared after the task is created successfully.
- The current selection card is rendered again after this card.

## Card: Processing Running

Text:

```text
Обработка
Материалов: 2

Статус: в работе
```

Optional progress line:

```text
Этап: распознавание речи
```

Buttons:

```text
[Отмена]
[Материалы]
[К подборке]
```

Rules:

- The watcher edits the task card in place.
- The current selection card remains the user's active control panel.
- Running task cards must not remove or hide `Обработать` from a new current selection.

## Card: Processing Cancel Requested

Text:

```text
Обработка
Материалов: 2

Статус: отмена запрошена
```

Buttons:

```text
[Материалы]
[К подборке]
```

Rules:

- `Отмена` is removed after cancellation has been requested.
- The card stays visible until the task becomes terminal.

## Card: Processing Succeeded

Text:

```text
Обработка
Материалов: 2

Статус: готово
Результат прикреплён ниже.
```

Buttons:

```text
[Результат]
[Материалы]
[Диагностика]
[Повторить]
[К подборке]
```

Rules:

- The produced artifact belongs to this task card.
- MVP behavior: send the result as a separate document message near the task card, preferably as a reply to the task card if Telegram behavior is stable.
- `Результат` resends or opens the produced artifact.
- `Диагностика` is shown when diagnostics exist or when the task was partial.
- `Повторить` starts from the same immutable snapshot in the first implementation stage.
- Do not put this result button on the current selection card.

## Card: Processing Failed

Text:

```text
Обработка
Материалов: 2

Статус: ошибка
Не удалось обработать материалы.
```

Buttons:

```text
[Диагностика]
[Материалы]
[Повторить]
[К подборке]
```

Rules:

- The error text must be short and actionable.
- Detailed failure reasons belong in `Диагностика`.
- The current selection remains independent and can continue collecting new materials.

## Card: Processing Canceled

Text:

```text
Обработка
Материалов: 2

Статус: отменено
```

Buttons:

```text
[Материалы]
[Повторить]
[К подборке]
```

Rules:

- Canceled tasks remain in recent history.
- `Повторить` is available because the task snapshot still exists.

## Recent Processing Navigation

The current selection card shows at most 3 recent processing tasks.

Preferred text:

```text
Последние обработки:
1. Готово · 2 материала · 14:25
2. В работе · 1 материал · 14:28
3. Ошибка · 4 материала · 14:31
```

Navigation options:

1. If a stable message link exists for the chat type, make each row a Telegram message link.
2. Otherwise show `[1] [2] [3]` buttons that open, refresh, or resend the corresponding task card.

Do not rely on message links as the only implementation path until private chat, group, forum topic, and Telegram client behavior are verified.

## Interaction Contract

### Adding Materials

When the user sends text, URL, file, voice, audio, video, photo, image, or document:

1. API creates media items and adds them to the current owner collection.
2. Telegram renders the current selection card as the latest bot message.
3. Existing task cards are not modified.

### Starting Processing

When the user taps `Обработать (N)`:

1. API creates an immutable selection snapshot from the current selection.
2. API creates an `analysis_run`.
3. Telegram creates a processing task card.
4. Telegram clears the current selection through the API collection mutation path.
5. Telegram renders the current selection card again as the latest bot message.
6. A watcher is attached to the task card, not to the current selection card.

### Updating Task Status

The watcher:

1. polls or receives task updates;
2. edits the task card status;
3. sends the result file near the task card on success;
4. does not overwrite the current selection card with task-specific controls.

### Opening Materials

From the current selection card:

- opens mutable current materials.

From a processing task card:

- opens immutable task snapshot materials.

The copy must make this distinction clear through context, not technical words.

### Repeating A Task

Stage 1 behavior:

- `Повторить` starts a new processing run from the same immutable selection snapshot.

Future behavior:

- offer two choices only if needed:
  - `Повторить`
  - `В новую подборку`

Do not add this complexity in the first implementation slice unless users ask for it.

## Accessibility And Telegram UX Rules

- Keep each card under roughly 10 visible lines when possible.
- Use short action labels.
- Keep the primary action as the last button row.
- Avoid raw IDs, versions, internal statuses, or technical nouns.
- Use status words consistently: `в очереди`, `в работе`, `отмена запрошена`, `готово`, `ошибка`, `отменено`.
- Destructive actions such as `Очистить` should be separated from the primary action and may require confirmation in a later stage.
- Avoid decorative emoji. The primary processing launch action is the plain text button `Обработать`.

## Persistent Interaction Surface State

Telegram needs durable interaction surface records to survive restarts.

Recommended interaction surface records:

- current selection card:
  - owner scope;
  - chat id;
  - thread id when applicable;
  - message id;
  - collection id;
  - version;
  - lifecycle status;
  - updated_at.

- processing task card:
  - analysis_run_id;
  - owner scope;
  - chat id;
  - thread id when applicable;
  - message id;
  - status;
  - result file message id when available;
  - updated_at.

The API remains the source of truth for media, collections, selections, runs, artifacts, and diagnostics. Interaction surface state is only the display mapping.

## Implementation Stages

### Stage 1: Text-Card Contract

Goal: ship the product model without risky Telegram media-edit behavior.

Scope:

- current selection card copy and buttons;
- separate processing task card;
- clear current selection after successful run creation;
- watcher edits task cards;
- result file sent as a separate document message;
- current selection card shows recent 3 task summaries;
- `[1] [2] [3]` fallback buttons for recent tasks.

Verification:

- Telegram tests prove current selection card remains current after task creation.
- Telegram tests prove a running task does not block a new current selection processing run.
- Runtime smoke proves a completed task sends its result file and leaves current selection usable.

### Stage 2: Durable Interaction Surfaces

Goal: survive bot restarts without losing which Telegram messages should be edited.

Scope:

- persist current selection card mapping;
- persist task card mapping;
- rebuild or supersede cards when stored messages cannot be edited;
- restore recent task navigation after restart.

Verification:

- restart test proves active task card continues updating after adapter restart.
- restart test proves current selection card can be restored or recreated as the latest card.

### Stage 3: Rich Result Attachment

Goal: improve result presentation after the base flow is stable.

Options:

- keep separate document message as the stable default;
- investigate editing a task card into a document/media message only if Telegram and aiogram behavior is reliable for the required chat types.

Verification:

- real Telegram runtime proof for private chat and group/forum contexts before making media-edit the default.

## Non-Goals

- Do not expose internal `selection`, `analysis_run`, `artifact`, or `buffer` vocabulary to users.
- Do not make the current selection card responsible for historical results.
- Do not block new processing runs solely because another task is active.
- Do not require message links for the MVP.
- Do not depend on Telegram fixed-bottom UI because the platform does not provide it.

## Open Questions

- Which chat types must be supported first: private only, groups, forum topics, or all?
- Should `Очистить` require confirmation in Telegram, or is button placement enough intentional friction?
- Should `Повторить` always reuse the same immutable snapshot, or should it copy materials back into the current selection in a later stage?
- What retention policy should apply to Telegram task cards and result file messages?
