# React Hero Section Pattern

## Overview

This skill documents the pattern for building responsive hero sections with call-to-action buttons using React, Tailwind CSS, and DaisyUI in this project.

## When to Use This Skill

Use this skill when users request:

- Building a hero section for a landing page
- Creating a header with gradient text and CTAs
- Implementing responsive button layouts
- Adding accessible navigation CTAs

## Core Capabilities

### 1. Gradient Text Headlines

Use Tailwind's gradient utilities with DaisyUI theme colors:
```css
bg-gradient-to-r from-primary to-secondary bg-clip-text text-transparent
```

### 2. Responsive CTA Layout

Mobile-first approach with Tailwind breakpoints:
- `flex flex-col` - Default stacked layout for mobile
- `sm:flex-row` - Side-by-side on screens >= 640px

### 3. DaisyUI Button Styling

- Primary CTA: `btn btn-primary btn-lg`
- Secondary CTA: `btn btn-outline btn-lg`

### 4. Accessibility

- `aria-labelledby="hero-headline"` on section for ARIA landmark
- `role="button"` on Link components for proper screen reader announcement
- Semantic HTML with proper heading hierarchy

## Full Component Implementation

```tsx
import { Link } from 'react-router-dom';

export function HeroSection() {
  return (
    <section
      className="min-h-screen flex items-center justify-center px-4 py-12 md:py-0"
      aria-labelledby="hero-headline"
    >
      <div className="max-w-4xl mx-auto text-center">
        <div className="flex flex-col items-center space-y-8">
          {/* Gradient headline */}
          <h1
            id="hero-headline"
            className="text-4xl md:text-5xl lg:text-6xl font-bold bg-gradient-to-r from-primary to-secondary bg-clip-text text-transparent"
          >
            Your Headline Here
          </h1>

          {/* Description */}
          <p className="text-lg md:text-xl text-base-content/80 max-w-2xl">
            Supporting description that explains the value proposition.
          </p>

          {/* CTAs - stacked on mobile, side-by-side on desktop */}
          <div className="flex flex-col sm:flex-row gap-4 w-full sm:w-auto justify-center">
            <Link to="/primary-action" className="btn btn-primary btn-lg" role="button">
              Primary CTA
            </Link>
            <Link to="/secondary-action" className="btn btn-outline btn-lg" role="button">
              Secondary CTA
            </Link>
          </div>
        </div>
      </div>
    </section>
  );
}
```

## Testing

Test the hero section using React Testing Library:

```tsx
import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';

const renderWithRouter = (ui: React.ReactElement) => {
  return render(<MemoryRouter>{ui}</MemoryRouter>);
};

describe('HeroSection', () => {
  it('renders headline with gradient styling', () => {
    renderWithRouter(<HeroSection />);
    const headline = screen.getByRole('heading', { level: 1 });
    expect(headline.className).toMatch(/bg-gradient-to-r/);
  });

  it('renders primary CTA linking to correct route', () => {
    renderWithRouter(<HeroSection />);
    const cta = screen.getByRole('button', { name: /get started/i });
    expect(cta).toHaveAttribute('href', '/register');
  });

  it('has responsive layout classes', () => {
    renderWithRouter(<HeroSection />);
    const container = screen.getByRole('button', { name: /get started/i }).parentElement;
    expect(container?.className).toMatch(/flex-col/);
    expect(container?.className).toMatch(/sm:flex-row/);
  });
});
```

## Best Practices

- Use DaisyUI theme tokens (`primary`, `secondary`) for gradient colors to support theming
- Always provide `aria-labelledby` on section elements for accessibility
- Use `role="button"` on Link components that act as CTAs
- Follow mobile-first responsive design with `flex-col` default

## File Locations

- Component: `frontend/src/components/home/HeroSection.tsx`
- Tests: `frontend/tests/unit/components/home/HeroSection.test.tsx`
- Barrel export: `frontend/src/components/home/index.ts`
