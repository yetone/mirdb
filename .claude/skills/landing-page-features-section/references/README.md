# Landing Page Features Section

## Overview

Create responsive features sections for product landing pages using CSS Grid with feature cards. Each card displays an icon, title, and description to highlight key value propositions.

## When to Use This Skill

Use this skill when:

- Building a product landing page features section
- Creating a "Why choose us" or "Key benefits" section
- Implementing feature cards with icons and descriptions
- Need responsive grid layout (3 cols → 2 cols → 1 col)

## HTML Structure

```html
<section id="features" class="features-section" data-testid="features-section">
  <div class="section-header">
    <h2 class="section-title">Why Choose MirDB?</h2>
    <p class="section-subtitle">Powerful features for modern applications</p>
  </div>
  <div class="features-grid" data-testid="features-grid">
    <div class="feature-card" data-testid="feature-card">
      <div class="feature-icon" data-testid="feature-icon">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <path d="M13 2 3 14h9l-1 8 10-12h-9l1-8z"></path>
        </svg>
      </div>
      <h3 class="feature-title" data-testid="feature-title">Fast Performance</h3>
      <p class="feature-description" data-testid="feature-description">
        Lightning-fast operations with optimized data structures.
      </p>
    </div>
    <!-- More feature cards (typically 3-6) -->
  </div>
</section>
```

## CSS Styles

```css
/* Features Section */
.features-section {
  padding: 5rem 2rem;
  background-color: #f8fafc;
}

.features-grid {
  max-width: 1200px;
  margin: 0 auto;
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 2rem;
}

.feature-card {
  background-color: #ffffff;
  padding: 2rem;
  border-radius: 8px;
  box-shadow: 0 4px 6px -1px rgb(0 0 0 / 0.1);
  transition: transform 0.2s, box-shadow 0.2s;
}

.feature-card:hover {
  transform: translateY(-4px);
  box-shadow: 0 10px 15px -3px rgb(0 0 0 / 0.1);
}

.feature-icon {
  width: 48px;
  height: 48px;
  margin-bottom: 1rem;
  color: #2563eb;
}

.feature-icon svg {
  width: 100%;
  height: 100%;
}

.feature-title {
  font-size: 1.25rem;
  font-weight: 600;
  color: #1e293b;
  margin-bottom: 0.75rem;
}

.feature-description {
  color: #64748b;
  font-size: 0.9375rem;
  line-height: 1.6;
}

/* Responsive breakpoints */
@media (max-width: 1024px) {
  .features-grid {
    grid-template-columns: repeat(2, 1fr);
  }
}

@media (max-width: 768px) {
  .features-grid {
    grid-template-columns: 1fr;
  }
}
```

## Best Practices

1. **Use 3-6 features** - Enough to show value without overwhelming
2. **Icons should be consistent** - Use same icon set/style throughout
3. **Titles should be action-oriented** - "Fast Performance" not just "Speed"
4. **Descriptions focus on benefits** - What users gain, not just features
5. **Add data-testid attributes** - For reliable test automation
6. **Use semantic HTML** - section, h2, h3 for accessibility/SEO

## Testing Pattern

```javascript
test('features section displays at least 3 features', () => {
  const featureCards = document.querySelectorAll('[data-testid="feature-card"]');
  expect(featureCards.length).toBeGreaterThanOrEqual(3);
});

test('each feature has icon, title, and description', () => {
  const cards = document.querySelectorAll('[data-testid="feature-card"]');
  cards.forEach(card => {
    expect(card.querySelector('[data-testid="feature-icon"]')).toBeInTheDocument();
    expect(card.querySelector('[data-testid="feature-title"]')).toBeInTheDocument();
    expect(card.querySelector('[data-testid="feature-description"]')).toBeInTheDocument();
  });
});
```

## Icon Options

Use inline SVGs for:
- Instant loading (no HTTP requests)
- CSS styling capability (color inherits from parent)
- Crisp rendering at any size

Popular icon sources:
- Lucide Icons (MIT license)
- Heroicons (MIT license)
- Feather Icons (MIT license)
