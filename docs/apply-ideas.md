# Apply Ideas

Technique: Compare recent behavior to a historical baseline to detect meaningful change.

Drift occurs when a system gradually changes its typical behavior.

A good dataset for this module:

- includes a historical period and a recent period
- represents repeated measurements of the same system

## Example Systems

### Website Performance

Possible fields:

- requests
- errors
- latency_ms

Questions to explore:

- Has error rate changed over time?
- Is average latency drifting upward?

### Retail Demand

Possible fields:

- date
- units_sold

Questions to explore:

- Has demand shifted compared to previous weeks?
- Are there gradual changes in purchasing behavior?

### Environmental Sensors

Possible fields:

- timestamp
- air_quality_index

Questions to explore:

- Is pollution increasing over time?
- Are recent readings different from historical patterns?

### System Resource Usage

Possible fields:

- cpu_usage
- memory_usage

Questions to explore:

- Has average system load changed?
- Does the system appear to be drifting toward higher resource usage?

### Transportation Demand

Possible fields:

- rides
- date

Questions to explore:

- Has ridership changed compared to earlier periods?
- Are changes gradual or sudden?
