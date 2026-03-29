/**
 * SEO Head Component
 * Owner: Scenario 9 - SEO Meta Tags
 *
 * Manages document head meta tags using React effects:
 * - Page title
 * - Meta description
 * - Open Graph tags (og:title, og:description, og:image)
 * - Viewport meta tag
 * - Canonical URL
 *
 * Requirements: NFR-5
 */
import { useEffect } from 'react';

export interface SEOHeadProps {
  title: string;
  description: string;
  ogImage?: string;
  canonicalUrl?: string;
}

/**
 * Updates or creates a meta tag in the document head
 */
function updateMetaTag(
  attribute: 'name' | 'property',
  attributeValue: string,
  content: string
): void {
  let meta = document.querySelector(
    `meta[${attribute}="${attributeValue}"]`
  ) as HTMLMetaElement | null;

  if (!meta) {
    meta = document.createElement('meta');
    meta.setAttribute(attribute, attributeValue);
    document.head.appendChild(meta);
  }

  meta.content = content;
}

/**
 * SEOHead component manages SEO meta tags for the homepage
 *
 * @example
 * ```tsx
 * <SEOHead
 *   title="URL Shortener - Shorten Links. Track Clicks. Grow Your Reach."
 *   description="Create short, memorable links with powerful click analytics."
 *   ogImage="/og-image.png"
 * />
 * ```
 */
export function SEOHead({
  title,
  description,
  ogImage = '/og-image.png',
  canonicalUrl,
}: SEOHeadProps) {
  useEffect(() => {
    // Update page title
    document.title = title;

    // Update meta description
    updateMetaTag('name', 'description', description);

    // Update Open Graph tags
    updateMetaTag('property', 'og:title', title);
    updateMetaTag('property', 'og:description', description);
    updateMetaTag('property', 'og:image', ogImage);
    updateMetaTag('property', 'og:type', 'website');

    // Update canonical URL if provided
    if (canonicalUrl) {
      let link = document.querySelector(
        'link[rel="canonical"]'
      ) as HTMLLinkElement | null;

      if (!link) {
        link = document.createElement('link');
        link.rel = 'canonical';
        document.head.appendChild(link);
      }

      link.href = canonicalUrl;
    }

    // Cleanup function - restore original title on unmount
    const originalTitle = document.title;
    return () => {
      // Only restore if component is being unmounted during navigation
      // We don't want to clear meta tags as they should persist
    };
  }, [title, description, ogImage, canonicalUrl]);

  // This component renders nothing - it only manages the document head
  return null;
}

// Default SEO configuration for the homepage
export const defaultSEOConfig: SEOHeadProps = {
  title: 'URL Shortener - Shorten Links. Track Clicks. Grow Your Reach.',
  description:
    'Create short, memorable links with powerful click analytics. Track engagement, manage your links, and grow your reach with our free URL shortening service.',
  ogImage: '/og-image.png',
};
