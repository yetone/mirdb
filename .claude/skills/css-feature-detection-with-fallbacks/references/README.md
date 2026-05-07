# CSS Feature Detection With Fallbacks

A small TypeScript module that wraps `CSS.supports()` for the homepage's cross-browser strategy. Used both at runtime (when a component wants to branch on engine support) and from Jest unit tests (to validate that the global stylesheet declares fallback chains).

## Lives at

`homepage/tests/unit/cross-browser/cssFeatureDetection.ts`

(It currently lives in the test folder because only tests and one runtime utility consume it. Promote to `homepage/src/lib/` if a non-test caller starts importing it.)

## API

```ts
supportsCSSFeature(property: string, value: string): boolean
supportsCSSVariables(): boolean
supportsFlexbox(): boolean
supportsGrid(): boolean
supportsScrollBehavior(): boolean
supportsBackdropFilter(): boolean        // probes both standard and -webkit-
checkCriticalCSSFeatures(): CriticalFeatureSupport
hasCriticalSupport(support): boolean
```

## Three rules baked into the implementation

1. **Defensive `CSS.supports` call.** Old browsers and SSR may not expose `CSS` or `CSS.supports`. We type-check both, then call inside try/catch because some engines throw on malformed property/value strings:

   ```ts
   if (typeof CSS === 'undefined' || typeof CSS.supports !== 'function') return false;
   try { return CSS.supports(property, value); } catch { return false; }
   ```

2. **`-webkit-backdrop-filter` is a valid yes.** Safari shipped this without dropping the prefix. `supportsBackdropFilter` returns true if either property reports support, so a Safari-style partial-support shape is treated as supported:

   ```ts
   return (
     supportsCSSFeature('backdrop-filter', 'blur(10px)') ||
     supportsCSSFeature('-webkit-backdrop-filter', 'blur(10px)')
   );
   ```

3. **`scroll-behavior` and `backdrop-filter` are optional.** `hasCriticalSupport` only requires `cssVariables`, `flexbox`, and `grid`. The other two have natural degradations: missing `scroll-behavior` becomes instant scroll; missing `backdrop-filter` falls back to the solid `background-color` we set on the header.

## Companion test pattern (static CSS analysis)

The Jest suite for this module also reads `globals.css`, `postcss.config.js`, and `tailwind.config.ts` and asserts contracts that would be hard to lint:

```ts
expect(globalsCss).toMatch(/var\(--/);                      // variables in use
expect(globalsCss).toMatch(/(monospace|sans-serif|serif)/); // generic family fallback
expect(globalsCss).toMatch(/scroll-behavior:\s*smooth/);    // smooth declared
expect(postcssConfig).toMatch(/autoprefixer/);              // autoprefixer wired
```

A risky-selector guard requires `@supports` whenever `:has()`, `color-mix()`, or `@property` appear:

```ts
const risky = [/:has\(/, /color-mix\(/, /@property\s/];
for (const re of risky) {
  if (re.test(globalsCss)) {
    expect(globalsCss).toMatch(/@supports/);
  }
}
```

This keeps progressive-enhancement enforced as a unit test rather than a code-review checklist.

## Mocking pattern in unit tests

The Jest tests replace `global.CSS` with a Jest mock in `beforeEach` and restore it in `afterEach`. Avoid polluting other suites by always pairing the override:

```ts
let originalCSS: typeof CSS | undefined;
beforeEach(() => {
  originalCSS = (global as unknown as { CSS?: typeof CSS }).CSS;
  (global as unknown as { CSS: { supports: jest.Mock } }).CSS = { supports: jest.fn() };
});
afterEach(() => { (global as unknown as { CSS?: typeof CSS }).CSS = originalCSS; });
```

The Safari shape test is worth keeping as a regression: it makes `CSS.supports` return false for the standard property and true for `-webkit-backdrop-filter`, then asserts `checkCriticalCSSFeatures().backdropFilter === true`.
