# React Component Testing

## Overview

Test React components in this project using Vitest + React Testing Library with jsdom environment. Follows the project's established patterns for unit and integration testing of homepage components.

## When to Use This Skill

Use this skill when users request:

- Writing unit tests for a React component
- Adding integration tests for user interactions
- Testing accessibility (ARIA labels, roles, alt text)
- Verifying link/button attributes (href, target, rel)
- Testing no-JavaScript fallback behavior

## Core Capabilities

### 1. Component Rendering Tests

Verify that components render expected content:

```tsx
import { render, screen } from '@testing-library/react';
import { describe, it, expect } from 'vitest';
import Hero from '../../src/components/Hero';

describe('Hero Section', () => {
  it('renders the product name', () => {
    render(<Hero />);
    expect(screen.getByRole('heading', { name: 'MirDB', level: 1 })).toBeInTheDocument();
  });

  it('renders the logo with alt text', () => {
    render(<Hero />);
    const logo = screen.getByAltText('MirDB logo');
    expect(logo).toHaveAttribute('src', '/assets/logo.gif');
  });
});
```

### 2. Accessibility Testing

Use ARIA roles and accessible names for queries:

```tsx
// Prefer role-based queries
screen.getByRole('link', { name: /get started/i });
screen.getByRole('region', { name: 'Hero section' });

// Verify security attributes
expect(link).toHaveAttribute('rel', 'noopener noreferrer');
expect(link).toHaveAttribute('target', '_blank');
```

### 3. User Interaction Testing

Test click behavior with @testing-library/user-event:

```tsx
import userEvent from '@testing-library/user-event';

it('navigates when CTA is clicked', async () => {
  const user = userEvent.setup();
  render(<Hero />);
  const link = screen.getByRole('link', { name: /get started/i });
  await user.click(link);
  expect(window.location.hash).toBe('#quick-start');
});
```

### 4. No-JS Graceful Degradation

Verify components work without JavaScript:

```tsx
it('uses anchor element for no-JS compatibility', () => {
  render(<Hero />);
  const link = screen.getByRole('link', { name: /get started/i });
  expect(link.tagName).toBe('A');
  expect(link).toHaveAttribute('href', '#quick-start');
});
```

## Best Practices

- Use `getByRole` over `getByText` or `getByTestId` for accessibility-first testing
- Prefer native HTML elements (`<a>`, `<button>`) over divs with onClick for no-JS fallback
- Use `@testing-library/user-event` (not `fireEvent`) for realistic interaction simulation
- Test security attributes (target, rel) on external links
- Keep tests focused on behavior, not implementation details

## Configuration

Test setup at `tests/setup.ts`:
```ts
import '@testing-library/jest-dom/vitest';
```

Vitest config in `vite.config.ts`:
```ts
test: {
  globals: true,
  environment: 'jsdom',
  setupFiles: './tests/setup.ts',
  css: true,
}
```

## Resources

### references/

- `README.md` - This documentation
