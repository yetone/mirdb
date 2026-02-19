# React Theme Context

## Overview

Pattern for implementing a dark mode theme system in React using Context API, with localStorage persistence and system color-scheme preference detection.

## When to Use This Skill

Use this skill when users request:

- "Add dark mode to the app"
- "Implement theme switching"
- "Persist user's theme preference"
- "Respect system color scheme"

## Core Capabilities

### 1. ThemeContext Provider

Create a context that provides theme state and toggle function:

```tsx
// src/context/ThemeContext.tsx
import { createContext, useContext, useEffect, useState, useCallback, useMemo } from 'react'
import type { ReactNode } from 'react'

type Theme = 'light' | 'dark'
const THEME_STORAGE_KEY = 'theme'

interface ThemeContextValue {
  theme: Theme
  toggleTheme: () => void
  setTheme: (theme: Theme) => void
}

const ThemeContext = createContext<ThemeContextValue | null>(null)

function getInitialTheme(): Theme {
  if (typeof window !== 'undefined') {
    const stored = localStorage.getItem(THEME_STORAGE_KEY)
    if (stored === 'light' || stored === 'dark') return stored
    if (window.matchMedia('(prefers-color-scheme: dark)').matches) return 'dark'
  }
  return 'light'
}

export function ThemeProvider({ children }: { children: ReactNode }) {
  const [theme, setThemeState] = useState<Theme>(getInitialTheme)

  useEffect(() => {
    document.documentElement.setAttribute('data-theme', theme)
    localStorage.setItem(THEME_STORAGE_KEY, theme)
  }, [theme])

  const toggleTheme = useCallback(() => {
    setThemeState((prev) => (prev === 'light' ? 'dark' : 'light'))
  }, [])

  const setTheme = useCallback((newTheme: Theme) => {
    setThemeState(newTheme)
  }, [])

  const value = useMemo(() => ({ theme, toggleTheme, setTheme }), [theme, toggleTheme, setTheme])

  return <ThemeContext.Provider value={value}>{children}</ThemeContext.Provider>
}

export function useTheme(): ThemeContextValue {
  const context = useContext(ThemeContext)
  if (!context) throw new Error('useTheme must be used within a ThemeProvider')
  return context
}
```

### 2. ThemeToggle Component

```tsx
// src/components/ui/ThemeToggle/ThemeToggle.tsx
import { useTheme } from '@/hooks/useTheme'
import { Icon } from '@/components/ui/Icon/Icon'

export function ThemeToggle() {
  const { theme, toggleTheme } = useTheme()
  const iconName = theme === 'dark' ? 'sun' : 'moon'
  const ariaLabel = theme === 'dark' ? 'Switch to light mode' : 'Switch to dark mode'

  return (
    <button onClick={toggleTheme} aria-label={ariaLabel} data-testid="theme-toggle" type="button">
      <Icon name={iconName} size={20} />
    </button>
  )
}
```

### 3. CSS Variables for Theming

```css
/* src/styles/variables.css */
:root {
  --color-background: #ffffff;
  --color-text: #0f172a;
  --color-surface: #f8fafc;
}

[data-theme="dark"] {
  --color-background: #0f172a;
  --color-text: #f1f5f9;
  --color-surface: #1e293b;
}
```

## Best Practices

- **Check localStorage first** for returning visitors, then system preference
- **Apply theme via data-theme attribute** on documentElement for CSS variable switching
- **Use sun/moon icons** that show what you'll switch TO (sun in dark mode = click for light)
- **Include proper ARIA labels** for accessibility
- **Listen for system preference changes** to update theme if no stored preference
- **Memoize context value** to prevent unnecessary re-renders

## Integration

Wrap your app with ThemeProvider in main.tsx:

```tsx
import { ThemeProvider } from './context/ThemeContext'

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <ThemeProvider>
      <App />
    </ThemeProvider>
  </React.StrictMode>,
)
```

## File Structure

```
src/
├── context/
│   └── ThemeContext.tsx     # Context provider and hook
├── hooks/
│   └── useTheme.ts          # Re-export for convenience
├── components/ui/
│   └── ThemeToggle/
│       ├── ThemeToggle.tsx
│       └── ThemeToggle.module.css
└── styles/
    ├── variables.css        # CSS custom properties with [data-theme] selectors
    └── themes/
        ├── light.css
        └── dark.css
```
