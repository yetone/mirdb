# react-sibling-rerender-isolation

Pattern for asserting that a state update in one component does not re-render an unrelated sibling, without coupling the assertion to `React.memo` internals.

## Problem

A common performance regression in React apps is a parent re-render cascading into expensive children. The naive way to test this is:

```tsx
expect(MyComponent.$$typeof).toBe(React.memo(() => null).$$typeof);
```

This tests the implementation, not the property. If someone refactors `React.memo` away in favour of a different optimisation (e.g. moving state down, splitting components), the test breaks even though the underlying property — "this child does not re-render when X changes" — still holds.

## Pattern

Use React's natural rendering contract: **state updates only re-render the owning component and its descendants; siblings are not touched.** Place the component under test and the state-owner as siblings under a stateless wrapper. The state change cannot propagate sideways unless React's invariant breaks.

```tsx
import { render, screen, fireEvent, act } from '@testing-library/react';
import { useState } from 'react';

it('does not re-render <FeaturesSection /> when an unrelated component updates state', () => {
  let featuresRenderCount = 0;

  function CountingFeatures() {
    featuresRenderCount += 1;
    return <FeaturesSection />;
  }

  function UnrelatedCounter() {
    const [count, setCount] = useState(0);
    return (
      <button
        type="button"
        data-testid="unrelated-counter-btn"
        onClick={() => setCount((c) => c + 1)}
      >
        count: {count}
      </button>
    );
  }

  function Wrapper() {
    return (
      <>
        <UnrelatedCounter />
        <CountingFeatures />
      </>
    );
  }

  render(<Wrapper />);

  const baseline = featuresRenderCount;
  expect(baseline).toBeGreaterThanOrEqual(1);

  const btn = screen.getByTestId('unrelated-counter-btn');
  act(() => { fireEvent.click(btn); });
  act(() => { fireEvent.click(btn); });
  act(() => { fireEvent.click(btn); });

  expect(btn.textContent).toMatch(/count: 3/);
  expect(featuresRenderCount).toBe(baseline);
});
```

## Why this works

- `UnrelatedCounter` owns `useState`. When `setCount` fires, React schedules a re-render of `UnrelatedCounter` only.
- The `Wrapper` is stateless; React has no reason to re-render it.
- Because `CountingFeatures` is rendered by `Wrapper` (not by `UnrelatedCounter`), it is not part of the subtree React re-renders.
- `featuresRenderCount` therefore stays at its post-mount baseline.

## When to use

- Performance regression tests for a component you do not own (and therefore cannot wrap in `React.memo` from the test file).
- Documenting an architectural invariant ("this section is stable when X changes") for future maintainers.
- Frontend perf budgets where the goal is to prevent re-render cascades rather than mandate a specific memoisation strategy.

## When not to use

- If the property you actually care about is "a parent's re-render does NOT propagate to this child" — in that case you need `React.memo` plus stable-props testing, because the natural contract gives the opposite guarantee.

## Failure mode interpretation

If this test fails (`featuresRenderCount > baseline`), the likely causes are:

1. Someone moved state up into the `Wrapper` — making it stateful and causing the whole subtree to re-render.
2. The "unrelated" component is no longer a sibling — it became an ancestor of the component under test.
3. A shared context provider was introduced that the component under test consumes — making any context change a re-render trigger.

Each of these is a real architectural change worth flagging.

## Sanity check

Always include a baseline assertion (`expect(baseline).toBeGreaterThanOrEqual(1)`) so a 0-render bug (component not actually mounted) does not silently pass the test.
