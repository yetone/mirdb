# React Features Section Pattern

## Overview

Pattern for creating feature showcase sections in React with multiple feature cards in a responsive grid, SVG icons for each feature, glassmorphism card styling, and proper accessibility.

## When to Use This Skill

Use this skill when users request:

- Building homepage feature highlights
- Creating feature showcase sections
- Implementing product capability displays
- Building service/benefit grid sections

## Core Capabilities

### 1. Data-Driven Feature Rendering

Define features as a typed array and render via map() for maintainability:

```tsx
interface Feature {
  id: string;
  title: string;
  description: string;
  icon: React.ReactNode;
}

const features: Feature[] = [
  {
    id: 'feature-1',
    title: 'Feature Title',
    description: 'Feature description here.',
    icon: <FeatureIcon />,
  },
  // ... more features
];
```

### 2. SVG Icon Components

Create inline SVG icons with accessibility attributes:

```tsx
const FeatureIcon = () => (
  <svg
    xmlns="http://www.w3.org/2000/svg"
    className="h-8 w-8"
    fill="none"
    viewBox="0 0 24 24"
    stroke="currentColor"
    strokeWidth={2}
    aria-hidden="true"  // Important for accessibility
  >
    <path strokeLinecap="round" strokeLinejoin="round" d="..." />
  </svg>
);
```

### 3. Glassmorphism Card Styling

Use these Tailwind classes for glass effect:

```
backdrop-blur-md bg-base-100/30 bg-opacity-30 border border-base-content/10 shadow-lg rounded-2xl
```

### 4. Responsive Grid Layout

Mobile-first responsive grid:

```
grid grid-cols-1 md:grid-cols-3 gap-6 md:gap-8
```

### 5. Complete Component Structure

```tsx
export function FeaturesSection() {
  return (
    <section
      className="py-16 md:py-24 px-4"
      aria-labelledby="features-heading"
    >
      <div className="max-w-6xl mx-auto">
        <h2 id="features-heading" className="text-3xl md:text-4xl font-bold text-center mb-12">
          Section Title
        </h2>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-6 md:gap-8" data-testid="features-grid">
          {features.map((feature) => (
            <article
              key={feature.id}
              className="backdrop-blur-md bg-base-100/30 bg-opacity-30 rounded-2xl p-6 md:p-8 border border-base-content/10 shadow-lg"
              data-testid="feature-card"
            >
              <div className="flex flex-col items-center text-center space-y-4">
                <div className="text-primary" data-testid="feature-icon">{feature.icon}</div>
                <h3 className="text-xl font-semibold">{feature.title}</h3>
                <p className="text-base-content/70">{feature.description}</p>
              </div>
            </article>
          ))}
        </div>
      </div>
    </section>
  );
}
```

## Best Practices

- Use `aria-hidden="true"` on decorative icons paired with text labels
- Use `article` elements for individual feature cards for semantic meaning
- Use `aria-labelledby` on the section element pointing to the heading
- Add `data-testid` attributes for testing
- Use mobile-first responsive classes (base then md: breakpoint)

## Testing Pattern

```tsx
it('displays exactly N feature cards', () => {
  render(<FeaturesSection />);
  const featureCards = screen.getAllByTestId('feature-card');
  expect(featureCards).toHaveLength(3);
});

it('each feature card contains an icon', () => {
  render(<FeaturesSection />);
  const featureCards = screen.getAllByTestId('feature-card');
  featureCards.forEach((card) => {
    const svg = card.querySelector('svg');
    expect(svg).toBeInTheDocument();
    expect(svg).toHaveAttribute('aria-hidden', 'true');
  });
});

it('has responsive grid classes', () => {
  render(<FeaturesSection />);
  const grid = screen.getByTestId('features-grid');
  expect(grid.className).toMatch(/grid-cols-1/);
  expect(grid.className).toMatch(/md:grid-cols-3/);
});
```

## Resources

### References

- `frontend/src/components/home/FeaturesSection.tsx` - Implementation example
- `frontend/tests/unit/components/home/FeaturesSection.test.tsx` - Test example
