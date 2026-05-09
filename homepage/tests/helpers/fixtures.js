/*
 * Owner: first builder.
 * Shared HTML fixtures and test constants.
 */

const SELECTORS = {
  hero: '#hero',
  heroTitle: '#hero h1',
  heroTagline: '#hero .tagline',
  heroCta: '#hero a.cta',
  features: '#features',
  install: '#install',
  usage: '#usage',
  architecture: '#architecture',
  resources: '#resources',
  footer: '#footer',
  nav: 'nav',
  themeToggle: '#theme-toggle',
};

const COPY_TEXTS = {
  projectName: 'MirDB',
  taglineParts: ['persistent key-value store', 'Memcached'],
  ctaText: 'Get Started',
  ctaHref: '#install',
};

module.exports = { SELECTORS, COPY_TEXTS };
