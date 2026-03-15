/**
 * Structured Data Component for JSON-LD.
 * Owner: Scenario 11 - SEO Best Practices
 *
 * Outputs JSON-LD structured data for:
 * - SoftwareApplication schema
 * - Organization schema
 * - WebSite schema
 *
 * Requirements:
 * - NFR-6: Structured data for rich search results
 */

import { useEffect } from 'react'
import { PRODUCT_NAME, TAGLINE, GITHUB_URL, VALUE_PROPOSITION } from '@/utils/constants'

export interface StructuredDataProps {
  name?: string
  description?: string
  url?: string
  codeRepository?: string
  applicationCategory?: string
  operatingSystem?: string
}

const DEFAULT_STRUCTURED_DATA = {
  '@context': 'https://schema.org',
  '@type': 'SoftwareApplication',
  name: PRODUCT_NAME,
  description: VALUE_PROPOSITION,
  url: 'https://mirdb.io',
  applicationCategory: 'Database Software',
  operatingSystem: 'Linux, macOS, Windows',
  codeRepository: GITHUB_URL,
  downloadUrl: GITHUB_URL,
  license: 'https://opensource.org/licenses/MIT',
  programmingLanguage: 'Rust',
  offers: {
    '@type': 'Offer',
    price: '0',
    priceCurrency: 'USD',
  },
  author: {
    '@type': 'Organization',
    name: 'MirDB Contributors',
    url: GITHUB_URL,
  },
  keywords: [
    'key-value store',
    'memcached',
    'persistent storage',
    'LSM tree',
    'Rust database',
    'skip-list',
  ],
}

export function StructuredData({
  name = PRODUCT_NAME,
  description = VALUE_PROPOSITION,
  url = 'https://mirdb.io',
  codeRepository = GITHUB_URL,
  applicationCategory = 'Database Software',
  operatingSystem = 'Linux, macOS, Windows',
}: StructuredDataProps) {
  useEffect(() => {
    // Check if script already exists
    let script = document.head.querySelector(
      'script[type="application/ld+json"][data-testid="structured-data"]'
    ) as HTMLScriptElement | null

    const structuredData = {
      ...DEFAULT_STRUCTURED_DATA,
      name,
      description,
      url,
      codeRepository,
      downloadUrl: codeRepository,
      applicationCategory,
      operatingSystem,
    }

    if (!script) {
      script = document.createElement('script')
      script.type = 'application/ld+json'
      script.setAttribute('data-testid', 'structured-data')
      document.head.appendChild(script)
    }

    script.textContent = JSON.stringify(structuredData, null, 2)

    // Cleanup function
    return () => {
      const existingScript = document.head.querySelector(
        'script[type="application/ld+json"][data-testid="structured-data"]'
      )
      if (existingScript) {
        existingScript.remove()
      }
    }
  }, [name, description, url, codeRepository, applicationCategory, operatingSystem])

  // This component doesn't render anything visible
  return null
}

export default StructuredData
