import { Helmet, HelmetData, HelmetProvider } from 'react-helmet-async';
import { BRAND } from '../../utils/constants';
import type { SEOMeta } from '../../types/homepage';

/**
 * SEO head manager for the homepage.
 * Owner: Scenario 8 - SEO Meta Tags and Discoverability.
 *
 * Uses react-helmet-async to inject SEO-friendly meta tags into the document
 * head: <title>, description, viewport, Open Graph, Twitter Card, and a
 * canonical link. Props are optional so the component renders correct defaults
 * for the homepage and can be reused on other pages by passing overrides.
 *
 * The component is resilient: it ships its own HelmetData so it still injects
 * tags when a parent HelmetProvider has not been mounted (for example, in
 * sibling-scenario tests that render <Home /> without wrapping it).
 */
export interface SEOTagsProps extends Partial<SEOMeta> {}

export const HOMEPAGE_SEO_DEFAULTS = {
  title: `${BRAND.name} - URL Shortening Service`,
  description:
    'MirDB is a fast URL shortening service with click analytics, link tracking, and a powerful management dashboard. Create short links and understand your audience.',
  canonicalUrl: 'https://mirdb.example.com/',
  ogImage: 'https://mirdb.example.com/og-image.png',
} as const;

const fallbackHelmetData = new HelmetData({}, HelmetProvider.canUseDOM);

export default function SEOTags(props: SEOTagsProps = {}) {
  const title = props.title ?? HOMEPAGE_SEO_DEFAULTS.title;
  const description = props.description ?? HOMEPAGE_SEO_DEFAULTS.description;
  const canonicalUrl = props.canonicalUrl ?? HOMEPAGE_SEO_DEFAULTS.canonicalUrl;
  const ogImage = props.ogImage ?? HOMEPAGE_SEO_DEFAULTS.ogImage;

  return (
    <Helmet helmetData={fallbackHelmetData}>
      <title>{title}</title>
      <meta name="description" content={description} />
      <meta name="viewport" content="width=device-width, initial-scale=1" />

      <meta property="og:title" content={title} />
      <meta property="og:description" content={description} />
      <meta property="og:type" content="website" />
      <meta property="og:url" content={canonicalUrl} />
      <meta property="og:image" content={ogImage} />
      <meta property="og:site_name" content={BRAND.name} />

      <meta name="twitter:card" content="summary_large_image" />
      <meta name="twitter:title" content={title} />
      <meta name="twitter:description" content={description} />
      <meta name="twitter:image" content={ogImage} />

      <link rel="canonical" href={canonicalUrl} />
    </Helmet>
  );
}
