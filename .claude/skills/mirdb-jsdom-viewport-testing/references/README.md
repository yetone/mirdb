# mirdb-jsdom-viewport-testing

Test viewport-aware React components in MirDB's Vitest + JSDOM setup. Reference implementation: the `useIsMobile` hook in `frontend/src/components/homepage/MobileMenu.tsx` and the `setViewport()` helper in `frontend/tests/responsive/homepage.responsive.test.tsx`.

## Why this skill exists

JSDOM does **not** load Tailwind's compiled CSS. Utility classes such as `hidden md:flex` or `lg:grid-cols-3` have no layout effect in tests. That means:

- Components that hide content with CSS media queries are still discoverable to Testing Library queries → `queryByRole(...).not.toBeInTheDocument()` will fail.
- Layout-dependent assertions (`getBoundingClientRect`, `offsetWidth`) return zero or stale values.
- The breakpoint a component reacts to must be driven by JavaScript that reads `window.innerWidth`, not by CSS.

The pattern below is how MirDB handles this consistently.

## The `useIsMobile` hook

Defined and exported from `frontend/src/components/homepage/MobileMenu.tsx`. Re-import it from there — do not duplicate.

```ts
export const MOBILE_BREAKPOINT_PX = 768;

export function useIsMobile(breakpoint: number = MOBILE_BREAKPOINT_PX): boolean {
  const getInitial = () =>
    typeof window !== 'undefined' && window.innerWidth < breakpoint;
  const [isMobile, setIsMobile] = useState<boolean>(getInitial);

  useEffect(() => {
    if (typeof window === 'undefined') return;
    const handleResize = () => setIsMobile(window.innerWidth < breakpoint);
    handleResize();
    window.addEventListener('resize', handleResize);
    return () => window.removeEventListener('resize', handleResize);
  }, [breakpoint]);

  return isMobile;
}
```

Key properties:

1. **Lazy `useState` initializer** — passing a function to `useState` runs only on first render and reads `window.innerWidth` synchronously. Tests can set `window.innerWidth` before `render()` and immediately assert on the DOM, no effect tick required.
2. **Resize listener inside `useEffect`** — covers the live resize case (browser drag, device rotation, or a test dispatching a synthetic `resize` event).
3. **`typeof window` guards** — SSR-safe, even though MirDB is SPA-only today.
4. **Re-import, don't duplicate** — `HomeNavbar` reads the same hook, so the two surfaces can never disagree at a breakpoint edge.

## Test helpers

### setViewport (synthetic resize)

```ts
import { act } from '@testing-library/react';

const setViewport = (width: number) => {
  Object.defineProperty(window, 'innerWidth', {
    configurable: true,
    writable: true,
    value: width,
  });
  // Mirror to documentElement.clientWidth for any code that reads it
  Object.defineProperty(document.documentElement, 'clientWidth', {
    configurable: true,
    writable: true,
    value: width,
  });
  act(() => {
    window.dispatchEvent(new Event('resize'));
  });
};
```

- Wrap the dispatch in `act()` so React batches the resulting `setState` into the next render before assertions run.
- Setting both `innerWidth` and `documentElement.clientWidth` covers helpers (some libraries read one or the other).
- `configurable: true` and `writable: true` so the same property can be redefined across tests.

### renderAtWidth (initial-width pattern)

For tests where the initial viewport matters more than a live resize:

```ts
const renderHome = (width: number) => {
  Object.defineProperty(window, 'innerWidth', {
    configurable: true,
    writable: true,
    value: width,
  });
  return render(<MemoryRouter initialEntries={['/']}><Home /></MemoryRouter>);
};
```

Setting `innerWidth` **before** calling `render()` is what makes the lazy useState initializer observe the correct width on first render.

### afterEach restore

```ts
const originalInnerWidth = window.innerWidth;
afterEach(() => {
  Object.defineProperty(window, 'innerWidth', {
    configurable: true,
    writable: true,
    value: originalInnerWidth,
  });
});
```

JSDOM persists the `innerWidth` override across tests in the same file, so restore it in `afterEach` to avoid cross-test pollution.

## Worked example — asserting the breakpoint swap

```ts
it('swaps from desktop nav to mobile menu when resizing from 1280px to 375px', () => {
  renderHome(1280);
  expect(screen.getByTestId('navbar-desktop-links')).toBeInTheDocument();
  expect(screen.queryByRole('button', { name: /menu/i })).not.toBeInTheDocument();

  setViewport(375);

  expect(screen.queryByTestId('navbar-desktop-links')).not.toBeInTheDocument();
  expect(screen.getByRole('button', { name: /menu/i })).toBeInTheDocument();
});
```

## Common mistakes

- Hiding the trigger with `hidden md:flex` and trying to assert `toBeEmptyDOMElement` on the container. The trigger is still in the DOM; the test will fail. Use conditional render (`if (!isMobile) return null`) or `{!isMobile && ...}` instead.
- Calling `dispatchEvent` without wrapping in `act()`. The `setState` runs but React schedules the re-render after the assertion runs, producing a stale tree.
- Forgetting `configurable: true` on the `Object.defineProperty` override — the second test in the same file then crashes with "Cannot redefine property".
- Reading `window.matchMedia` instead of `innerWidth`. `matchMedia` exists in JSDOM but does not respond to `Object.defineProperty(window, 'innerWidth', ...)` changes unless you also mock `matchMedia` itself. `innerWidth` is the simpler primitive.

## Related skills

- `mirdb-mobile-disclosure-menu` — for the disclosure pattern that consumes `useIsMobile`
- `mirdb-responsive-test-partitioning` — for how to structure responsive integration tests using these helpers
