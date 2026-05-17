# React Helmet Async SEO

## Overview

This skill captures the MirDB project pattern for declaratively injecting SEO and social-sharing meta tags into the document head with react-helmet-async. The pattern centralises default copy in an exported constant tied to the shared BRAND constant so the rendered title stays aligned with the hero h1, navbar brand, and Home.tsx fallback.

## When to Use This Skill

Use this skill when users request:

- "Add SEO meta tags to a page"
- "Set the document title / description from a React component"
- "Add Open Graph / Twitter Card tags for social previews"
- "Add a canonical URL"
- "Make a page discoverable to search engines"

## Core Capabilities

### 1. Tag set to render

Every public marketing page in the MirDB frontend should declare the following tags through a single `<Helmet>` element:

- `<title>` - page title, prefer `${BRAND.name} - <page descriptor>` format
- `<meta name="description">` - 1-2 sentences, mention the product domain (URL shortening, analytics, etc.)
- `<meta name="viewport" content="width=device-width, initial-scale=1">` - required for responsive Tailwind breakpoints on mobile
- Open Graph: `og:title`, `og:description`, `og:type` (use `website` for landing pages), `og:url`, `og:image`, `og:site_name`
- Twitter Card: `twitter:card` set to `summary_large_image`, plus `twitter:title`, `twitter:description`, `twitter:image`
- `<link rel="canonical" href={...}>` - production homepage URL

### 2. Defaults exported as a constant

Export a `HOMEPAGE_SEO_DEFAULTS` (or page-equivalent) `as const` object so tests can reference exactly the same strings the component renders. Build the title from the shared `BRAND` constant in `frontend/src/utils/constants.ts` so renames propagate.

```tsx
export const HOMEPAGE_SEO_DEFAULTS = {
  title: `${BRAND.name} - URL Shortening Service`,
  description:
    'MirDB is a fast URL shortening service with click analytics, link tracking, and a powerful management dashboard. Create short links and understand your audience.',
  canonicalUrl: 'https://mirdb.example.com/',
  ogImage: 'https://mirdb.example.com/og-image.png',
} as const;
```

### 3. Prop-based overrides

Props mirror a `Partial<SEOMeta>` interface so non-production deployments or other pages can override copy without duplicating the component. Apply `??` to fall back to defaults.

### 4. Reference implementation

See `frontend/src/components/homepage/SEOTags.tsx` for the canonical implementation.

## Best Practices

- Always centralise the title-building logic on `BRAND.name` rather than hardcoding the product name a second time
- Mirror Open Graph and Twitter Card values: both protocols accept the same image+copy, no need to diverge
- Use `og:type="website"` for landing pages; use `article` for blog/content pages
- Use `summary_large_image` Twitter Card for landing pages that have a hero image
- Canonical URL should be the production homepage, overridable via props for previews
- Keep description ≥ 20 chars and mention the product domain so search engines have something useful to display

## Resources

### references/
- `README.md` - This documentation

### Related skills
- `helmet-async-provider-resilient` - How to make a Helmet-using component work without a HelmetProvider wrapper (matters for sibling-scenario tests)
- `helmet-async-testing` - How to test a react-helmet-async-driven component with Vitest + JSDOM
