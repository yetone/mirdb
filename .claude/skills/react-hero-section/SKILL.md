---
name: react-hero-section
description: Pattern for building responsive hero sections with CTAs using React, Tailwind CSS, and DaisyUI
---

# React Hero Section Pattern

Use this skill when building hero sections or landing page headers with:
- Gradient text headlines
- Call-to-action buttons with navigation
- Responsive layouts (mobile stacked, desktop side-by-side)
- Accessibility features (ARIA landmarks, semantic HTML)

## Key Features

- **Gradient Text**: Use `bg-gradient-to-r from-primary to-secondary bg-clip-text text-transparent`
- **Responsive CTAs**: Use `flex flex-col sm:flex-row` for mobile-first stacking
- **Link as Button**: Use React Router `<Link>` with `role="button"` for navigation CTAs
- **ARIA Landmarks**: Add `aria-labelledby` to section elements

## Quick Start

```tsx
import { Link } from 'react-router-dom';

export function HeroSection() {
  return (
    <section aria-labelledby="hero-headline">
      <h1 id="hero-headline" className="bg-gradient-to-r from-primary to-secondary bg-clip-text text-transparent">
        Your Headline
      </h1>
      <p>Supporting description</p>
      <div className="flex flex-col sm:flex-row gap-4">
        <Link to="/register" className="btn btn-primary btn-lg" role="button">
          Get Started
        </Link>
        <Link to="/login" className="btn btn-outline btn-lg" role="button">
          Log In
        </Link>
      </div>
    </section>
  );
}
```

See [README.md](references/README.md) for full documentation.
