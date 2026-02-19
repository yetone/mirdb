# React Polymorphic Button

## Overview

Create a reusable Button component that renders as either a `<button>` or `<a>` element based on whether an `href` prop is provided. This pattern ensures proper semantic HTML and accessibility for both interactive buttons and navigation links.

## When to Use This Skill

Use this skill when users request:

- "Create a button component that can also be a link"
- "Build a CTA button that supports both onClick and href"
- "Make a button that opens external links in new tabs"

## Core Capabilities

### 1. Polymorphic Rendering

The component conditionally renders based on props:

```tsx
export function Button({
  variant = 'primary',
  children,
  href,
  onClick,
  target,
  rel,
  className,
  'aria-label': ariaLabel,
}: ButtonProps) {
  const combinedClassName = cn(styles.button, styles[variant], className)

  if (href) {
    return (
      <a
        href={href}
        className={combinedClassName}
        target={target}
        rel={rel || (target === '_blank' ? 'noopener noreferrer' : undefined)}
        aria-label={ariaLabel}
      >
        {children}
      </a>
    )
  }

  return (
    <button
      type="button"
      className={combinedClassName}
      onClick={onClick}
      aria-label={ariaLabel}
    >
      {children}
    </button>
  )
}
```

### 2. Automatic Security Attributes

When `target="_blank"` is set, automatically adds `rel="noopener noreferrer"` to prevent tabnabbing attacks:

```tsx
rel={rel || (target === '_blank' ? 'noopener noreferrer' : undefined)}
```

### 3. TypeScript Interface

```tsx
export interface ButtonProps {
  variant?: 'primary' | 'secondary'
  children: React.ReactNode
  href?: string
  onClick?: () => void
  target?: string
  rel?: string
  className?: string
  'aria-label'?: string
}
```

## Best Practices

- Always add `type="button"` to button elements to prevent accidental form submission
- Use `aria-label` for buttons with icon-only content
- Ensure minimum touch target size of 44x44 pixels for mobile accessibility
- Use CSS Modules for scoped styling with variant classes

## Usage Examples

### Primary Button (action)
```tsx
<Button variant="primary" onClick={handleSubmit}>
  Submit
</Button>
```

### Secondary Link Button
```tsx
<Button variant="secondary" href="/about">
  Learn More
</Button>
```

### External Link with Icon
```tsx
<Button
  variant="secondary"
  href="https://github.com/repo"
  target="_blank"
  aria-label="View on GitHub"
>
  <GitHubIcon /> View on GitHub
</Button>
```

## File Structure

```
src/components/ui/Button/
├── Button.tsx          # Component implementation
├── Button.test.tsx     # Unit tests
└── Button.module.css   # Scoped styles
```
