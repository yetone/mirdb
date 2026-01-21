---
name: landing-page-features-section
description: Create a features section for landing pages with responsive CSS Grid cards containing icons, titles, and descriptions. Use when building product/marketing landing pages.
scope: project
---

# Landing Page Features Section Pattern

Create responsive features sections for landing pages using CSS Grid with cards containing icons, titles, and descriptions.

## Quick Start

HTML structure for feature cards in a 3-column grid:

```html
<section id="features" class="features-section" data-testid="features-section">
  <div class="section-header">
    <h2 class="section-title">Key Features</h2>
    <p class="section-subtitle">What makes us different</p>
  </div>
  <div class="features-grid" data-testid="features-grid">
    <div class="feature-card" data-testid="feature-card">
      <div class="feature-icon"><!-- SVG icon --></div>
      <h3 class="feature-title">Feature Title</h3>
      <p class="feature-description">Feature benefit description</p>
    </div>
    <!-- More feature cards... -->
  </div>
</section>
```

See [README.md](references/README.md) for full documentation.
