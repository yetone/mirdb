# MirDB Theme Context Persistence

## Overview

The MirDB frontend stores the active DaisyUI theme in a React Context that hydrates from `localStorage` on mount and applies the value as a `data-theme` attribute on `document.documentElement`. This skill captures the exact pattern used in `frontend/src/contexts/ThemeContext.tsx` and `frontend/src/components/shared/ThemeToggle.tsx`, including the validation, fallback, and provider-less safety conventions that the test suite relies on.

## When to Use This Skill

Use this skill when:

- Adding a new theme to the application or extending the `THEMES` constant.
- Building a theme picker / multi-theme menu (not just light/dark toggle).
- Writing tests that need to seed or read the theme via `localStorage` / `data-theme`.
- Touching any component that consumes `useTheme()` from `frontend/src/contexts/ThemeContext.tsx`.

## Core Capabilities

### 1. Persisting and hydrating theme via localStorage

The provider reads `localStorage('theme')` once on initial state, narrows it via the `isValidTheme` type guard, and falls back to `DEFAULT_THEME` (`'light'`) for missing, malformed, or unknown values. A single `useEffect` keyed on the active theme then handles both DOM application and storage write-through:

```tsx
useEffect(() => {
  applyTheme(theme);
  try {
    window.localStorage.setItem(THEME_STORAGE_KEY, theme);
  } catch {
    // Storage may be unavailable (private mode, quota); ignore.
  }
}, [theme]);
```

The pattern keeps the DOM, React state, and storage in lockstep without manual coordination at each call site.

### 2. DaisyUI integration via data-theme

DaisyUI selects palettes by reading the `data-theme` attribute on a parent element. The provider writes it to `document.documentElement`:

```tsx
function applyTheme(theme: Theme) {
  if (typeof document !== 'undefined' && document.documentElement) {
    document.documentElement.setAttribute('data-theme', theme);
  }
}
```

Tests assert against `document.documentElement.getAttribute('data-theme')`, so any new theme work must continue to write that attribute (not a class, not a style tag).

### 3. Safe default for provider-less rendering

`useTheme()` returns a module-level `defaultContextValue` when no `<ThemeProvider>` is mounted. This lets consumers like `<ThemeToggle />` render in isolation (e.g. in unit tests) without throwing or silently corrupting global state:

```tsx
const defaultContextValue: ThemeContextValue = {
  theme: DEFAULT_THEME,
  setTheme: () => {},
  toggleTheme: () => {},
};

const ThemeContext = createContext<ThemeContextValue>(defaultContextValue);

export function useTheme(): ThemeContextValue {
  return useContext(ThemeContext);
}
```

When extending the API, preserve this safety property — do not throw if a component is rendered outside a provider.

### 4. Test conventions for theme state

`frontend/tests/components/shared/ThemeToggle.test.tsx` resets state in both `beforeEach` and `afterEach`:

```ts
function resetThemeEnvironment() {
  try {
    window.localStorage.clear();
  } catch {
    // ignore
  }
  document.documentElement.removeAttribute('data-theme');
}
```

When writing a new test that touches theme, copy this pattern verbatim — both halves are needed because the persistence effect may otherwise repaint `data-theme` between tests.

## Best Practices

- Always validate stored values with `isValidTheme()` before applying them. Never trust raw `localStorage` reads.
- Funnel persistence and DOM application through a single `useEffect`. Do not call `setItem` and `setAttribute` from multiple sites.
- Wrap every `localStorage` access in `try/catch`. Storage may be unavailable in private mode or when quota is exceeded.
- Keep the `<ThemeToggle />` two-state (light/dark). A multi-theme picker can call `setTheme` directly without changing the toggle contract.
- Preserve the provider-less safe default. Throwing on missing provider would break the unit test for isolated rendering.
- Expose meaningful test hooks on accessible elements: `aria-label`, `aria-pressed`, and `data-active-theme` are all asserted by the suite.

## Resources

### references/

- `README.md` - This documentation
