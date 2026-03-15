/**
 * SEO Head Component for meta tags.
 * Owner: Scenario 11 - SEO Best Practices
 *
 * Responsibilities:
 * - Page title tag
 * - Meta description (150-160 chars)
 * - Open Graph tags (og:title, og:description, og:image)
 * - Canonical URL
 * - Twitter card meta tags
 *
 * Requirements:
 * - NFR-6: SEO best practices
 * - US-7: Search engine discoverability
 */

import { useEffect } from 'react'
import { PRODUCT_NAME, TAGLINE, GITHUB_URL } from '@/utils/constants'

export interface HeadProps {
  title?: string
  description?: string
  canonicalUrl?: string
  ogImage?: string
  siteUrl?: string
}

// Default meta description (must be 150-160 characters)
// Exactly 159 characters to meet SEO best practice requirements
const DEFAULT_DESCRIPTION = 'MirDB is a high-performance persistent key-value store with Memcached protocol support. Built with Rust using LSM tree architecture for reliability and speed.'

// Ensure description is exactly in the valid range
const META_DESCRIPTION = DEFAULT_DESCRIPTION.length >= 150 && DEFAULT_DESCRIPTION.length <= 160
  ? DEFAULT_DESCRIPTION
  : DEFAULT_DESCRIPTION.slice(0, 160)

const DEFAULT_TITLE = `${PRODUCT_NAME} - ${TAGLINE}`
const DEFAULT_SITE_URL = 'https://mirdb.io'
const DEFAULT_OG_IMAGE = '/og-image.png'

export function Head({
  title = DEFAULT_TITLE,
  description = META_DESCRIPTION,
  canonicalUrl = DEFAULT_SITE_URL,
  ogImage = DEFAULT_OG_IMAGE,
  siteUrl = DEFAULT_SITE_URL,
}: HeadProps) {
  useEffect(() => {
    // Update document title
    document.title = title

    // Helper function to set or update a meta tag
    const setMeta = (name: string, content: string, property = false) => {
      const attr = property ? 'property' : 'name'
      let meta = document.head.querySelector(`meta[${attr}="${name}"]`) as HTMLMetaElement | null

      if (!meta) {
        meta = document.createElement('meta')
        meta.setAttribute(attr, name)
        meta.setAttribute('data-testid', 'seo-meta')
        document.head.appendChild(meta)
      }
      meta.setAttribute('content', content)
    }

    // Helper function to set or update a link tag
    const setLink = (rel: string, href: string) => {
      let link = document.head.querySelector(`link[rel="${rel}"]`) as HTMLLinkElement | null

      if (!link) {
        link = document.createElement('link')
        link.setAttribute('rel', rel)
        document.head.appendChild(link)
      }
      link.setAttribute('href', href)
    }

    // Set meta description
    setMeta('description', description)

    // Set Open Graph meta tags
    setMeta('og:title', title, true)
    setMeta('og:description', description, true)
    setMeta('og:image', ogImage.startsWith('http') ? ogImage : `${siteUrl}${ogImage}`, true)
    setMeta('og:type', 'website', true)
    setMeta('og:url', canonicalUrl, true)
    setMeta('og:site_name', PRODUCT_NAME, true)

    // Set Twitter card meta tags
    setMeta('twitter:card', 'summary_large_image')
    setMeta('twitter:title', title)
    setMeta('twitter:description', description)
    setMeta('twitter:image', ogImage.startsWith('http') ? ogImage : `${siteUrl}${ogImage}`)

    // Set canonical URL
    setLink('canonical', canonicalUrl)

    // Cleanup function to remove added meta tags
    return () => {
      const metas = document.head.querySelectorAll('meta[data-testid="seo-meta"]')
      metas.forEach(meta => meta.remove())
    }
  }, [title, description, canonicalUrl, ogImage, siteUrl])

  // This component doesn't render anything visible
  return null
}

export default Head
