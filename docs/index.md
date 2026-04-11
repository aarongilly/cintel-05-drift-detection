
# Continuous Intelligence

## Custom Project - Data Journal Drift

### Dataset
For the custom project I used a subset of my [Data Journal](https://datajournal.guide) aggregated to the week. I'm using a slightly more robust version of this same file for my Capstone project also being done through NW Missouri State University. So it was thematic.

**Columns:**

- **week** - week the data represents.
- **satisfaction** - average of daily "how good was today on a scale of 10?" ratings.
- **health** - average of daily "how healthy do I feel on a scale of 10" ratings.
- **sleepduration** - weekly average as measured by Oura ring - time asleep.
- **hr** - weekly average as measured by Oura ring - lowest HR overnight.
- **hrv** - weekly average as measured by Oura ring - Heart Rate Variability.
- **readinesss** - score derived by Oura ring, 0 to 100.
- **activity** - score derived by Oura ring, 0 to 100.
- **sleep** - score derived by Oura ring, 0 to 100.
- **activecals** -  - weekly average as measured by Oura ring - all calories burned (estimate).

### Signals
I actually did *not* create an signals with this. The data contained everything I needed directly.

### Experiments
My entire dataset was a modification from the original example. I played around some with the thresholds, trying to define numbers that felt fair and accurate. If I had more time I'd probably have tried out a smaller period and done a rolling drift detection scheme of some sort, but at the moment I'm trying to leave it here.

### Results
The modification was incredibly straightforward. This pattern (and the example) is quite robust and easy to change. The results of the data show almost no drift in any of the metrics.

This makes some sense though, I'm averaging over a **more than year** for the "current period". So it only shows quite broad trends.

Still though, I did find that my average **activity** (a score measured by Oura) **is drifting** even at this broad level.

### Interpretation
With such a wide timespan in the "current" window, I didn't expect to see much drift. I set the drift thresholds to what would actually indicate to me, personally, a meaningful change over time.

The only result that is drifting is **activity**.

That makes sense, as I did up my workout game mid-last year, which as more or less been carried forward since then. So that's good news!

I learned the nice pattern here of capturing into CSV a simple set of figures for each metric:

1. Reference average
2. Current average
3. Calculated delta
4. Is drifting flag

Personally I think this is a great set of diagnostics to capture. You could quite easily have not included the two averages, but I think they provide a nice context. You could also have not included the calculated delta, but again that's just adding more work to the reader. I like this pattern.

# Original Index Content

---

This site provides documentation for this project.
Use the navigation to explore module-specific materials.

## How-To Guide

Many instructions are common to all our projects.

See
[⭐ **Workflow: Apply Example**](https://denisecase.github.io/pro-analytics-02/workflow-b-apply-example-project/)
to get these projects running on your machine.

## Project Documentation Pages (docs/)

- **Home** - this documentation landing page
- **Project Instructions** - instructions specific to this module
- **Your Files** - how to copy the example and create your version
- **Glossary** - project terms and concepts

## Additional Resources

- [Suggested Datasets](https://denisecase.github.io/pro-analytics-02/reference/datasets/cintel/)
