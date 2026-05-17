# mirdb-responsive-test-partitioning

Structure responsive integration tests for MirDB pages. Reference implementation: `frontend/tests/responsive/homepage.responsive.test.tsx`.

## When to use

- A `scenario.json` test plan calls for multiple viewport widths in the same file
- You want a single integration file to drive the page at desktop / tablet / mobile / 320px and assert the layout swap
- You need to assert that a page does not introduce horizontal overflow at 320px under JSDOM, which cannot compute real layout

## File location and naming

- Path: `frontend/tests/responsive/<page>.responsive.test.tsx`
- Each page that has REQ-7-style responsive coverage gets one file
- File imports the page (`Home`, `BookDetail`, etc.) and renders it inside `<MemoryRouter>` so React Router links resolve to `href` attributes

## Suite layout (one describe per scenario step)

The describe-block names mirror the test case ids in `scenario.json`. This makes failures readable in CI and keeps test-to-spec traceability obvious during review.

```tsx
describe('<Page> responsive layout (REQ-7, US-5)', () => {
  describe('Test case 1: desktop viewport (>= 1024px)', () => {
    beforeEach(() => { renderHome(1280); });
    it('shows the inline nav links', () => { /* ... */ });
    it('does not render a hamburger menu button', () => { /* ... */ });
    it('renders the desktop-only multi-column grid', () => { /* ... */ });
  });

  describe('Test case 2: mobile viewport (< 768px)', () => {
    beforeEach(() => { renderHome(375); });
    it('renders the hamburger trigger', () => { /* ... */ });
    it('hides the inline desktop links from the DOM', () => { /* ... */ });
  });

  describe('Test case 3: opening the mobile menu', () => { /* userEvent flow */ });
  describe('Test case 4: closing the mobile menu with Escape', () => { /* userEvent flow */ });

  describe('Test case 5: smallest expected mobile viewport (320px)', () => {
    beforeEach(() => { renderHome(320); });
    it('renders the page without throwing horizontal overflow on key layout containers', () => { /* see scanner below */ });
    it('uses responsive utility classes (max-w-*, w-full, px-*) on layout containers', () => { /* ... */ });
    it('does not require a horizontal scrollbar', () => { /* ... */ });
  });

  describe('Tablet viewport (768-1023px)', () => {
    it('still renders the inline nav at 820px', () => { /* ... */ });
  });

  describe('Resize integration', () => {
    it('swaps from desktop to mobile when resizing from 1280px to 375px', () => { /* ... */ });
    it('swaps from mobile to desktop when resizing from 375px to 1280px', () => { /* ... */ });
  });
});
```

## The 320px inline-width scanner

JSDOM does not compute layout, so `document.documentElement.scrollWidth` is unreliable. The real regression to catch is someone hard-coding an inline pixel width on a section that overflows 320px. This scanner walks the rendered DOM and flags inline `width:` or `min-width:` values larger than 320px:

```ts
it('renders the page without throwing horizontal overflow on key layout containers', () => {
  const home = screen.getByTestId('home-main');
  expect(home).toBeInTheDocument();

  const widthOffenders: string[] = [];
  const isPxWidthLargerThan320 = (value: string | null): boolean => {
    if (!value) return false;
    const match = value.match(/^(\d+(?:\.\d+)?)px$/);
    if (!match) return false;
    return Number(match[1]) > 320;
  };

  home.querySelectorAll<HTMLElement>('*').forEach((el) => {
    const style = el.getAttribute('style');
    if (!style) return;
    const widthMatch = style.match(/(?:^|;)\s*(?:min-)?width\s*:\s*([^;]+)/i);
    if (widthMatch && isPxWidthLargerThan320(widthMatch[1].trim())) {
      widthOffenders.push(
        `${el.tagName.toLowerCase()}#${el.id || el.getAttribute('data-testid') || ''}: ${widthMatch[1]}`
      );
    }
  });

  expect(widthOffenders).toEqual([]);
});
```

Why this works:

- It only inspects `style="..."` inline declarations (Tailwind utilities live in classNames JSDOM ignores, but inline widths from a future regression *will* land on the DOM).
- It accepts percentages, `auto`, `fit-content`, etc. (the regex requires a pure pixel value).
- The offender list embeds tag + id/testid so failures point directly at the problem element.

## The "uses responsive utility classes" sibling assertion

Tailwind classNames are inert in JSDOM but they are still in `el.className`. A regex test catches regressions where someone deletes the responsive prefix entirely:

```ts
it('uses responsive utility classes on layout containers', () => {
  const hero = screen.getByTestId('hero');
  const features = screen.getByTestId('features-section');
  expect(hero.className).toMatch(/px-/);
  expect(features.className).toMatch(/px-/);
});
```

Pair with `min-h-[44px]` / `min-w-[44px]` literal-string assertions on touch-target controls.

## Resize integration block

Drives a real `innerWidth` change inside `act()` so the resize-listener path is exercised end-to-end. See `mirdb-jsdom-viewport-testing` for the `setViewport()` helper. The two key cases:

1. Render at desktop, resize to mobile → desktop list disappears, hamburger appears
2. Render at mobile (with menu possibly open), resize to desktop → hamburger disappears, desktop list appears, any open menu state is reset

The second case is what catches stale open state — an effect in the component must collapse `open` when `isMobile` flips to `false`.

## Common mistakes

- Putting every viewport assertion in one big `it()` block. Failures become unreadable. One describe per breakpoint band; one `it()` per assertion.
- Relying on `document.documentElement.scrollWidth` to detect overflow at 320px. JSDOM returns 0/garbage for this. Use the inline-width scanner above.
- Forgetting the `afterEach` `innerWidth` restore — see `mirdb-jsdom-viewport-testing`.
- Asserting against Tailwind utilities with `toHaveStyle({ display: 'none' })`. The class is there but the style is not computed in JSDOM. Assert against the className string instead.

## Related skills

- `mirdb-jsdom-viewport-testing` — required for the `setViewport()` and `renderAtWidth()` helpers
- `mirdb-mobile-disclosure-menu` — implementation pattern these tests exercise
