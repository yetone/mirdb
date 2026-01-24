/**
 * SEO Utility Functions
 * Owner: Scenario 13 - SEO Optimization
 *
 * Utilities for managing SEO meta tags and document head.
 * These functions manipulate the document head directly for SEO purposes.
 */

export interface OGTags {
  title: string
  description: string
  image?: string
  url?: string
  type?: string
  siteName?: string
}

export interface SEOConfig {
  title: string
  description: string
  canonicalUrl?: string
  ogTags?: Partial<OGTags>
}

/**
 * Sets the page title in the document head
 * @param title - The title to set
 */
export function setPageTitle(title: string): void {
  document.title = title
}

/**
 * Sets or creates a meta description tag
 * @param description - The meta description content
 */
export function setMetaDescription(description: string): void {
  let metaDescription = document.querySelector('meta[name="description"]')

  if (!metaDescription) {
    metaDescription = document.createElement('meta')
    metaDescription.setAttribute('name', 'description')
    document.head.appendChild(metaDescription)
  }

  metaDescription.setAttribute('content', description)
}

/**
 * Sets Open Graph meta tags for social media sharing
 * @param tags - The Open Graph tags to set
 */
export function setOpenGraphTags(tags: OGTags): void {
  const ogProperties: Record<string, string | undefined> = {
    'og:title': tags.title,
    'og:description': tags.description,
    'og:image': tags.image,
    'og:url': tags.url,
    'og:type': tags.type || 'website',
    'og:site_name': tags.siteName,
  }

  Object.entries(ogProperties).forEach(([property, content]) => {
    if (content === undefined) return

    let metaTag = document.querySelector(`meta[property="${property}"]`)

    if (!metaTag) {
      metaTag = document.createElement('meta')
      metaTag.setAttribute('property', property)
      document.head.appendChild(metaTag)
    }

    metaTag.setAttribute('content', content)
  })
}

/**
 * Sets or creates a canonical URL link tag
 * @param url - The canonical URL
 */
export function setCanonicalUrl(url: string): void {
  let canonicalLink = document.querySelector('link[rel="canonical"]')

  if (!canonicalLink) {
    canonicalLink = document.createElement('link')
    canonicalLink.setAttribute('rel', 'canonical')
    document.head.appendChild(canonicalLink)
  }

  canonicalLink.setAttribute('href', url)
}

/**
 * Initializes all SEO tags with default landing page values
 * @param config - SEO configuration object
 */
export function initializeSEO(config: SEOConfig): void {
  setPageTitle(config.title)
  setMetaDescription(config.description)

  if (config.canonicalUrl) {
    setCanonicalUrl(config.canonicalUrl)
  }

  if (config.ogTags) {
    setOpenGraphTags({
      title: config.ogTags.title || config.title,
      description: config.ogTags.description || config.description,
      image: config.ogTags.image,
      url: config.ogTags.url || config.canonicalUrl,
      type: config.ogTags.type,
      siteName: config.ogTags.siteName,
    })
  }
}

/**
 * Default SEO configuration for the landing page
 */
export const DEFAULT_LANDING_SEO: SEOConfig = {
  title: 'URL Shortener - Shorten URLs & Track Analytics',
  description: 'Create short, memorable links with powerful analytics. Track clicks, monitor performance, and share your success with our free URL shortening service.',
  canonicalUrl: typeof window !== 'undefined' ? window.location.origin : undefined,
  ogTags: {
    title: 'URL Shortener - Shorten URLs & Track Analytics',
    description: 'Create short, memorable links with powerful analytics. Track clicks, monitor performance, and share your success.',
    type: 'website',
    siteName: 'URL Shortener',
  },
}

/**
 * Custom hook for setting SEO on component mount
 * Can be used in React components to set SEO on mount
 */
export function useSEO(config: SEOConfig = DEFAULT_LANDING_SEO): void {
  if (typeof window !== 'undefined') {
    initializeSEO(config)
  }
}
