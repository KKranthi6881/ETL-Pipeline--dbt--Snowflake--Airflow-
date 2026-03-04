# Model Documentation Standards

## Purpose
These instructions control how DuckCode generates documentation for dbt models and other data assets when you use Auto Docs or ask for documentation in chat.

## Style
- Default to **concise, high-signal** documentation.
- Use business-friendly language, but avoid unnecessary fluff.
- Prefer short sections and bullet points over long paragraphs.

## Detail Levels
- When the user explicitly asks for **full documentation**, you may include:
  - A brief executive summary (2–3 sentences).
  - A business narrative explaining what the model does and how data flows.
  - 3–7 key transformation steps.
  - The most important business rules and downstream impact.
- When the user asks for a **quick overview** or "short docs":
  - Provide only 1–2 short sections.
  - Skip detailed transformation cards and long narratives.

## YAML / dbt-specific Guidance
- Keep YAML fields JSON-serializable and avoid markdown formatting inside values.
- Do not invent columns, metrics, or models that do not exist.
- Keep description fields around 200–400 characters unless the user requests more detail.
- Use clear, stable field names so that generated YAML remains easy to edit by hand.

## When In Doubt
- If instructions in this file conflict with hard-coded examples in the tool, prefer this file.
- Adapt tone and level of detail based on the user’s request (short vs detailed docs).
