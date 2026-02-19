# React External Links

## Overview

Pattern for implementing secure external links in React applications that open in new tabs while preventing tab-nabbing security vulnerabilities. All external links must use `target="_blank"` with `rel="noopener noreferrer"` attributes.

## When to Use This Skill

Use this skill when users request:

- External links in navigation, footer, or any component
- Links to GitHub, documentation, or third-party resources
- CI/CD status badges that link to external services
- Social media links or external profile links

## Core Capabilities

### 1. Secure External Link Pattern

Every external link that opens in a new tab must include both security attributes:

```tsx
<a
  href="https://github.com/yetone/mirdb"
  target="_blank"
  rel="noopener noreferrer"
  className={styles.link}
>
  GitHub Repository
</a>
```

**Why both attributes?**
- `target="_blank"` opens link in new tab
- `rel="noopener"` prevents new page from accessing `window.opener`
- `rel="noreferrer"` also prevents referrer header from being sent

### 2. External Link with Accessibility

Include `aria-label` when link text doesn't describe destination:

```tsx
<a
  href={CIRCLECI_PIPELINE_URL}
  target="_blank"
  rel="noopener noreferrer"
  aria-label="View CircleCI build status"
  data-testid="circleci-badge-link"
>
  <img
    src={CIRCLECI_BADGE_URL}
    alt="CircleCI build status"
  />
</a>
```

### 3. Testing External Links

Use React Testing Library to verify security attributes:

```tsx
it('has external links with target="_blank" and rel="noopener noreferrer"', () => {
  render(<Footer />)

  const githubLink = screen.getByTestId('github-link')
  expect(githubLink).toHaveAttribute('target', '_blank')
  expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer')
})
```

### 4. E2E Testing with Playwright

Verify link opens in new tab:

```ts
test('GitHub link opens repository in new tab', async ({ page, context }) => {
  const githubLink = page.locator('[data-testid="github-link"]')

  // Verify attributes
  await expect(githubLink).toHaveAttribute('target', '_blank')
  await expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer')

  // Verify new tab opens
  const [newPage] = await Promise.all([
    context.waitForEvent('page'),
    githubLink.click()
  ])

  await expect(newPage).toHaveURL(/github\.com\/yetone\/mirdb/i)
})
```

## Best Practices

- Always use both `noopener` AND `noreferrer` in the `rel` attribute
- Add `data-testid` attributes for easy testing
- Include `aria-label` when link content is non-textual (e.g., images, icons)
- Use constants for URLs (e.g., `GITHUB_URL`) to avoid duplication
- Test both unit (attribute presence) and E2E (actual behavior)

## Example Usage

From this project's Footer component:

```tsx
import { GITHUB_URL } from '@/utils/constants'

// In component
<a
  href={GITHUB_URL}
  target="_blank"
  rel="noopener noreferrer"
  className={styles.link}
  data-testid="github-link"
>
  GitHub Repository
</a>
```

## Resources

### references/

- `README.md` - This documentation

### Related Files

- `homepage/src/components/layout/Footer/Footer.tsx` - Implementation example
- `homepage/src/components/layout/Footer/Footer.test.tsx` - Test examples
- `homepage/tests/e2e/footer.spec.ts` - E2E test examples
