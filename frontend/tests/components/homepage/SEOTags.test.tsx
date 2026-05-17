import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { render, cleanup, waitFor } from '@testing-library/react';
import { HelmetProvider } from 'react-helmet-async';
import SEOTags, { HOMEPAGE_SEO_DEFAULTS } from '../../../src/components/homepage/SEOTags';

const renderSEO = (ui: React.ReactNode = <SEOTags />) =>
  render(<HelmetProvider>{ui}</HelmetProvider>);

const getMetaByName = (name: string): HTMLMetaElement | null =>
  document.head.querySelector(`meta[name="${name}"]`);
const getMetaByProperty = (prop: string): HTMLMetaElement | null =>
  document.head.querySelector(`meta[property="${prop}"]`);
const getLinkByRel = (rel: string): HTMLLinkElement | null =>
  document.head.querySelector(`link[rel="${rel}"]`);

const stripManagedHead = () => {
  document.head.querySelectorAll('title, meta, link').forEach((el) => el.remove());
};

describe('SEOTags', () => {
  beforeEach(() => {
    stripManagedHead();
  });

  afterEach(() => {
    cleanup();
    stripManagedHead();
  });

  it('renders a <title> containing "MirDB" and "URL Shortening" (test case 1)', async () => {
    renderSEO();

    await waitFor(() => {
      expect(document.title).toMatch(/mirdb/i);
    });
    expect(document.title).toMatch(/url shortening/i);
    expect(document.title).toBe(HOMEPAGE_SEO_DEFAULTS.title);
  });

  it('renders a description meta tag with non-empty content describing the service (test case 2)', async () => {
    renderSEO();

    await waitFor(() => {
      const desc = getMetaByName('description');
      expect(desc).not.toBeNull();
    });
    const desc = getMetaByName('description');
    const content = desc?.getAttribute('content') ?? '';
    expect(content.length).toBeGreaterThan(20);
    expect(content).toMatch(/url|short|analytics|link/i);
  });

  it('renders Open Graph og:title and og:description matching the page title and description (test case 3)', async () => {
    renderSEO();

    await waitFor(() => {
      expect(getMetaByProperty('og:title')).not.toBeNull();
    });
    const ogTitle = getMetaByProperty('og:title');
    const ogDescription = getMetaByProperty('og:description');
    const ogType = getMetaByProperty('og:type');

    expect(ogTitle).not.toBeNull();
    expect(ogDescription).not.toBeNull();
    expect(ogType).not.toBeNull();

    const ogTitleContent = ogTitle?.getAttribute('content') ?? '';
    const ogDescriptionContent = ogDescription?.getAttribute('content') ?? '';
    const ogTypeContent = ogType?.getAttribute('content') ?? '';

    expect(ogTitleContent.length).toBeGreaterThan(0);
    expect(ogDescriptionContent.length).toBeGreaterThan(0);
    expect(ogTitleContent).toBe(HOMEPAGE_SEO_DEFAULTS.title);
    expect(ogDescriptionContent).toBe(HOMEPAGE_SEO_DEFAULTS.description);
    expect(ogTypeContent).toBe('website');
  });

  it('renders a viewport meta tag with width=device-width and initial-scale=1 (test case 4)', async () => {
    renderSEO();

    await waitFor(() => {
      expect(getMetaByName('viewport')).not.toBeNull();
    });
    const viewport = getMetaByName('viewport');
    const content = viewport?.getAttribute('content') ?? '';
    expect(content).toContain('width=device-width');
    expect(content).toContain('initial-scale=1');
  });

  it('renders a canonical link pointing to the production homepage URL (test case 5)', async () => {
    renderSEO();

    await waitFor(() => {
      expect(getLinkByRel('canonical')).not.toBeNull();
    });
    const canonical = getLinkByRel('canonical');
    const href = canonical?.getAttribute('href') ?? '';
    expect(href).toMatch(/^https?:\/\//);
    expect(href).toBe(HOMEPAGE_SEO_DEFAULTS.canonicalUrl);
  });

  it('renders a Twitter Card meta tag for social previews', async () => {
    renderSEO();

    await waitFor(() => {
      expect(getMetaByName('twitter:card')).not.toBeNull();
    });
    const card = getMetaByName('twitter:card');
    const content = card?.getAttribute('content') ?? '';
    expect(content.length).toBeGreaterThan(0);
    expect(['summary', 'summary_large_image']).toContain(content);
  });

  it('allows callers to override the title via props', async () => {
    const customTitle = 'Custom MirDB Title - URL Shortening Pro';
    renderSEO(<SEOTags title={customTitle} />);

    await waitFor(() => {
      expect(document.title).toBe(customTitle);
    });
    const ogTitle = getMetaByProperty('og:title');
    expect(ogTitle?.getAttribute('content')).toBe(customTitle);
  });

  it('allows callers to override the canonical URL via props', async () => {
    const customUrl = 'https://preview.mirdb.example.com/';
    renderSEO(<SEOTags canonicalUrl={customUrl} />);

    await waitFor(() => {
      const canonical = getLinkByRel('canonical');
      expect(canonical?.getAttribute('href')).toBe(customUrl);
    });
  });
});
