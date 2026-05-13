# React Card Navigation Links

## Overview

This skill covers building card-based navigation link components in React with Tailwind CSS, following the MirDB homepage patterns. Cards use native anchor elements for keyboard accessibility, group-hover for coordinated hover effects, and data from shared constants.

## When to Use This Skill

Use this skill when users request:

- Creating documentation or resource link cards
- Building card-based navigation sections
- Adding hover and focus states to card components
- Making link cards keyboard accessible
- Testing card components with Vitest and Testing Library

## Core Capabilities

### 1. Card Component Structure

Each card is a native `<a>` element wrapping icon, heading, description, and visual indicator:

```tsx
<a
  href={link.url}
  target="_blank"
  rel="noopener noreferrer"
  className="group block rounded-xl border border-gray-200 bg-white p-6
    transition-all duration-200
    hover:border-brand-400 hover:shadow-md
    focus-visible:border-brand-500 focus-visible:shadow-md focus-visible:outline-none
    dark:border-gray-800 dark:bg-gray-950"
>
  <div className="...group-hover:bg-brand-100...">
    {IconComponent && <IconComponent />}
  </div>
  <h3>{link.title}</h3>
  <p>{link.description}</p>
  <span className="...group-hover:translate-x-0.5...">Learn more →</span>
</a>
```

Key patterns:
- `group` on the anchor enables `group-hover` on children
- `focus-visible:` classes mirror hover styling for keyboard users
- Native `<a>` element provides inherent keyboard accessibility

### 2. Data-Driven Rendering

Documentation links are defined in shared constants, not hardcoded:

```typescript
// src/types/index.ts
export interface DocLink {
  id: string;
  title: string;
  description: string;
  url: string;
  icon: string; // Lucide icon name
}

// src/utils/constants.ts
export const DOC_LINKS: DocLink[] = [
  {
    id: 'readme',
    title: 'Project Overview',
    description: 'README with project goals, features, and getting started guide.',
    url: GITHUB_REPO_URL,
    icon: 'BookOpen',
  },
  // ... more links
];
```

### 3. Dynamic Icon Resolution

Lucide icons are resolved from string names:

```tsx
import * as Icons from 'lucide-react';

const IconComponent = (Icons as Record<string, React.ComponentType<{ className?: string }>>)[link.icon];
```

### 4. Responsive Grid Layout

Cards use Tailwind responsive grid:

```tsx
<div className="mt-10 grid gap-6 md:grid-cols-3">
  {DOC_LINKS.map((link) => (/* card */))}
</div>
```

### 5. Testing Card Components

Test with Vitest + React Testing Library:

```tsx
import { render, screen } from '@testing-library/react';
import { describe, it, expect } from 'vitest';

describe('Docs component', () => {
  it('renders at least 3 documentation cards', () => {
    render(<Docs />);
    const cards = screen.getAllByRole('link');
    expect(cards.length).toBeGreaterThanOrEqual(3);
  });

  it('each card has hover classes', () => {
    render(<Docs />);
    const cards = screen.getAllByRole('link');
    for (const card of cards) {
      const hasHover = card.className.includes('hover:border')
        || card.className.includes('hover:shadow')
        || card.className.includes('group-hover:');
      expect(hasHover).toBe(true);
    }
  });

  it('cards are keyboard focusable', () => {
    render(<Docs />);
    const cards = screen.getAllByRole('link');
    for (const card of cards) {
      const hasFocus = card.className.includes('focus-visible:');
      expect(hasFocus).toBe(true);
    }
  });
});
```

## Best Practices

- Use native `<a>` elements, not `<div onClick>` — ensures keyboard accessibility
- Match `focus-visible` styles to hover styles for equivalent visual feedback
- Use `group`/`group-hover` pattern for coordinated child hover effects
- Define link data in shared constants, typed via shared interfaces
- Set `target="_blank"` and `rel="noopener noreferrer"` for external links
- Add `aria-label` on section elements for screen reader landmarks
- Use `section` with `aria-label` instead of `aria-labelledby` for simpler setup
- Test CSS classes directly (className.includes) rather than visual appearance
- Use `getAllByRole('link')` to count and iterate card elements

## Resources

### references/

- `README.md` - This documentation
