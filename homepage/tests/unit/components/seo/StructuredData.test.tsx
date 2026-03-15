import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { render, cleanup } from '@testing-library/react'
import { StructuredData } from '@/components/seo/StructuredData'

describe('StructuredData Component', () => {
  beforeEach(() => {
    // Clear any existing structured data scripts from previous tests
    const existingScripts = document.head.querySelectorAll('script[type="application/ld+json"]')
    existingScripts.forEach(script => script.remove())
  })

  afterEach(() => {
    cleanup()
    // Clean up structured data scripts after each test
    const scripts = document.head.querySelectorAll('script[type="application/ld+json"]')
    scripts.forEach(script => script.remove())
  })

  // Test Case 7: JSON-LD script with SoftwareApplication schema is present
  it('adds JSON-LD script with SoftwareApplication schema', () => {
    render(<StructuredData />)

    const scripts = document.head.querySelectorAll('script[type="application/ld+json"]')
    expect(scripts.length).toBeGreaterThan(0)

    // Find the SoftwareApplication schema
    let foundSoftwareApp = false
    scripts.forEach(script => {
      try {
        const data = JSON.parse(script.textContent || '{}')
        if (data['@type'] === 'SoftwareApplication' ||
            (Array.isArray(data['@graph']) && data['@graph'].some((item: { '@type': string }) => item['@type'] === 'SoftwareApplication'))) {
          foundSoftwareApp = true
        }
      } catch {
        // Ignore parse errors
      }
    })

    expect(foundSoftwareApp).toBe(true)
  })

  it('includes @context schema.org in JSON-LD', () => {
    render(<StructuredData />)

    const script = document.head.querySelector('script[type="application/ld+json"]')
    expect(script).not.toBeNull()

    const data = JSON.parse(script?.textContent || '{}')
    expect(data['@context']).toBe('https://schema.org')
  })

  it('includes application name MirDB in structured data', () => {
    render(<StructuredData />)

    const script = document.head.querySelector('script[type="application/ld+json"]')
    expect(script).not.toBeNull()

    const data = JSON.parse(script?.textContent || '{}')
    expect(data.name).toContain('MirDB')
  })

  it('includes application category in structured data', () => {
    render(<StructuredData />)

    const script = document.head.querySelector('script[type="application/ld+json"]')
    expect(script).not.toBeNull()

    const data = JSON.parse(script?.textContent || '{}')
    expect(data.applicationCategory).toBeTruthy()
  })

  it('includes operating system in structured data', () => {
    render(<StructuredData />)

    const script = document.head.querySelector('script[type="application/ld+json"]')
    expect(script).not.toBeNull()

    const data = JSON.parse(script?.textContent || '{}')
    expect(data.operatingSystem).toBeTruthy()
  })

  it('includes code repository URL in structured data', () => {
    render(<StructuredData />)

    const script = document.head.querySelector('script[type="application/ld+json"]')
    expect(script).not.toBeNull()

    const data = JSON.parse(script?.textContent || '{}')
    // Could be in codeRepository or sourceCode
    expect(data.codeRepository || data.downloadUrl).toContain('github.com')
  })

  it('renders without visual output in the DOM', () => {
    const { container } = render(<StructuredData />)

    // Component should not render any visible elements
    expect(container.textContent).toBe('')
  })
})
