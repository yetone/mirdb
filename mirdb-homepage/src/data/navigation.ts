/**
 * Navigation data for MirDB Homepage.
 * Owner: Scenario 6 - Documentation Links
 */

import { NavigationLink } from '../types';
import { DOCS_LINKS } from '../utils/constants';

export const documentationLinks: NavigationLink[] = [
  {
    id: 'architecture',
    label: 'Architecture',
    href: DOCS_LINKS.architecture,
    external: true,
    description: 'Learn about MirDB\'s LSM-tree architecture and internal design.',
  },
  {
    id: 'api-reference',
    label: 'API Reference',
    href: DOCS_LINKS.apiReference,
    external: true,
    description: 'Complete API documentation for all MirDB operations.',
  },
  {
    id: 'configuration',
    label: 'Configuration',
    href: DOCS_LINKS.configuration,
    external: true,
    description: 'Configuration options and tuning parameters for MirDB.',
  },
];
