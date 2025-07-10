# Explicit research program grounded in a few core principles and questions

## 1. Core Research Questions for Soccer/NWSL Analytics

### 1. What truly generates goals?
- Which on‐ball actions (shots from various zones, through-balls, crosses) contribute most to goal probability?

- How do off-ball movements (pressing, runs behind the defense) alter those probabilities?

### 2. How do events translate into points (wins/draws)?

- What’s the relationship between goal differential and expected points earned?

- How do situational factors (e.g. trailing vs. leading, time remaining) shift the leverage of each action?

### 3. What is “replacement level” in soccer?

- How do you define a “replacement” attacker, midfielder, defender or GK in terms of baseline contributions to xG, xA, defensive actions, etc.?

- How many “plus-minus” or “expected points above replacement” does a championship-caliber roster need?

### 4. How do we separate skill from luck?

- Which metrics stabilize quickly (e.g. shot‐creation per 90) and which regress heavily (e.g. finishing conversion)?

- What sample sizes are needed to get reliable indicators of finishing, defending, or goalkeeping skill?

### 5. How do context and environment modulate performance?

- How do home vs. away, pitch condition, weather, altitude or travel fatigue affect xG, passing completion, defensive success?

- How do league‐wide trends (e.g. pace of play, pressing intensity) evolve year-to-year, and how to adjust for era shifts?

### 6. How do defensive and transition contributions work?

- How many expected goals does a successful press or turnover prevent?

- What’s the value of progressive carries vs. long balls vs. midfield interceptions?

## 2. Example Tool Names & Descriptions

| Tool Name                     | Description                                                                                                       |
| ----------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| **ExpectedGoalsCalculator**   | Computes the xG of any shot or sequence, based on location, build-up, and player context.                         |
| **ShotQualityProfiler**       | Breaks down shot data by zone, assist type, body part, pressure level, and goalkeeper position.                   |
| **PassNetworkAnalyzer**       | Constructs and visualizes passing graphs; identifies key connectors and space-creation links.                     |
| **WinExpectancyModule**       | Estimates a team’s probability of taking 3/1/0 points at any game state, given current score, time, and location. |
| **ReplacementValueEstimator** | Calculates each player’s “expected points above replacement” per 90 minutes in each role.                         |
| **StabilityAssessor**         | Runs statistical tests (e.g. split-half reliabilities) to show which metrics require more samples.                |
| **ContextNormalizer**         | Adjusts raw stats for home/away, opponent strength, weather, and NWSL season variability.                         |
| **DefensiveImpactTracker**    | Measures disruptions (pressures, interceptions, clearances) and converts them to prevented-xG.                    |



## 3. Draft Server Prompt

```sql
You are “NWSL Knowledge Engine,” a remote MCP server that supplies objective, data-driven answers about the National Women’s Soccer League.  
When a user asks a question:
- Identify which research question it maps to (e.g. xG, win expectancy, replacement level, context adjustment).
- Invoke the appropriate tool (e.g. ExpectedGoalsCalculator, PassNetworkAnalyzer) with clear parameter names.
- Return both the raw numbers and a concise interpretation (“That sequence carried an xG of 0.17, meaning it should produce a goal about once every six tries.”).
- If the question spans multiple areas, combine tools and synthesize the outputs.
- Always cite data sources and time-stamps (e.g. “Data through July 2025, regular season games only”).  
Keep responses precise, jargon-light, and focused on “objective knowledge” as defined by these core questions.
```

