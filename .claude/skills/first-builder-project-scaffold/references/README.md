# First Builder Project Scaffold

## Overview

Initialize a Vite + React + TypeScript + Tailwind CSS project as the first scenario builder. Creates the complete project structure with shared resources (types, utilities, hooks, layout) and stub components for all remaining scenarios to enable parallel development.

## When to Use This Skill

Use this skill when users request:

- Setting up a new frontend project from scratch
- Creating a Vite + React + TypeScript project
- Initializing project structure with Tailwind CSS
- Creating shared resources for multi-scenario development
- Generating stub components for parallel scenario work

## Core Capabilities

### 1. Project Initialization

```bash
npm create vite@latest <project-name> -- --template react-ts
cd <project-name>
npm install
npm install tailwindcss @tailwindcss/vite vitest @testing-library/react \
  @testing-library/jest-dom @testing-library/user-event jsdom clsx tailwind-merge
```

### 2. Vite Configuration

Configure Vite with Tailwind plugin and Vitest:

```ts
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

export default defineConfig({
  plugins: [react(), tailwindcss()],
  test: {
    globals: true,
    environment: 'jsdom',
    setupFiles: './tests/setup.ts',
    css: true,
  },
})
```

### 3. Shared Resources

Create these files as the first builder:

| File | Purpose |
|------|---------|
| `src/types/index.ts` | Shared TypeScript interfaces (MirDBFeature, ComparisonRow, etc.) |
| `src/utils/cn.ts` | Tailwind class merge utility (clsx + tailwind-merge) |
| `src/utils/constants.ts` | App-wide constants (URLs, repo links) |
| `src/components/Layout/index.tsx` | Page shell with Header, Footer, skip-to-content link |
| `src/styles/globals.css` | Tailwind directives and base theme tokens |

### 4. Stub Components

Create placeholder components with proper `id` attributes for anchor navigation:

```tsx
// Example stub for QuickStart (Scenario 4)
export default function QuickStart() {
  return (
    <section id="quick-start" className="py-16 px-4 max-w-7xl mx-auto">
      <h2 className="text-3xl font-bold text-center mb-8">Quick Start</h2>
      {/* Implemented by Scenario 4 */}
    </section>
  );
}
```

### 5. Folder Structure

```
src/
├── components/
│   ├── Hero/          # Scenario 1
│   ├── Features/      # Scenario 2
│   ├── Demo/          # Scenario 3
│   ├── QuickStart/    # Scenario 4
│   ├── Docs/          # Scenario 5
│   ├── Comparison/    # Scenario 6
│   ├── Roadmap/       # Scenario 7
│   ├── GitHubLink/    # Scenario 8
│   ├── ThemeToggle/   # Scenario 9
│   ├── Layout/        # Shared
│   └── SEO/           # Scenario 12
├── hooks/             # Shared hooks
├── utils/             # Shared utilities
├── types/             # Shared types
└── styles/            # Global styles
tests/
├── unit/              # Component unit tests
├── integration/       # Integration tests
└── setup.ts           # Test setup (jest-dom matchers)
```

## Best Practices

- Each stub component must have an `id` attribute matching anchor targets
- Use `export default` for all components (consistent pattern)
- CSS smooth scroll on `<html>` for no-JS graceful degradation
- All interactive elements use native HTML (`<a>`, `<button>`) not divs with onClick
- Assets (logo.gif, usage.gif) go in `public/assets/`
- Test setup imports `@testing-library/jest-dom/vitest` for DOM matchers

## Resources

### references/

- `README.md` - This documentation
