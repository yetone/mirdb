/**
 * External links data for the homepage.
 * Owner: Scenario 6 - External Links and Resources
 *
 * Requirements:
 * - GitHub repository link (REQ-6)
 * - Documentation/README link
 * - Memcached protocol specification link
 */

import type { ExternalLink } from '../types'

/** GitHub repository URL for MirDB */
export const GITHUB_URL = 'https://github.com/theseus-rs/mirdb'

/** Documentation/README URL */
export const DOCS_URL = 'https://github.com/theseus-rs/mirdb#readme'

/** Official Memcached protocol specification URL */
export const PROTOCOL_URL = 'https://github.com/memcached/memcached/blob/master/doc/protocol.txt'

/**
 * All external links for the homepage.
 * These links are used across header, footer, and resource sections.
 */
export const links: ExternalLink[] = [
  {
    label: 'GitHub',
    url: GITHUB_URL,
    type: 'github',
  },
  {
    label: 'Documentation',
    url: DOCS_URL,
    type: 'docs',
  },
  {
    label: 'Memcached Protocol',
    url: PROTOCOL_URL,
    type: 'protocol',
  },
]

/**
 * Get link by type
 */
export const getLinkByType = (type: ExternalLink['type']): ExternalLink | undefined => {
  return links.find((link) => link.type === type)
}
