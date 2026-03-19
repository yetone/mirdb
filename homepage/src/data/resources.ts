/**
 * Resources data for MirDB Homepage.
 * Owner: Scenario 6 - Resources and Links Section
 *
 * Contains the six resource links displayed in the Resources section.
 */

import type { Resource } from '@/types';
import { GITHUB_URL } from '@/lib/constants';

export const resources: Resource[] = [
  {
    title: 'Documentation',
    description: 'Learn how to install, configure, and use MirDB effectively.',
    href: `${GITHUB_URL}#readme`,
    external: true,
  },
  {
    title: 'API Reference',
    description: 'Explore the memcached protocol commands supported by MirDB.',
    href: `${GITHUB_URL}/blob/master/docs/api.md`,
    external: true,
  },
  {
    title: 'Examples',
    description: 'Browse example configurations and usage patterns.',
    href: `${GITHUB_URL}/tree/master/examples`,
    external: true,
  },
  {
    title: 'Contributing Guide',
    description: 'Contribute to MirDB development and help improve the project.',
    href: `${GITHUB_URL}/blob/master/CONTRIBUTING.md`,
    external: true,
  },
  {
    title: 'Issue Tracker',
    description: 'Report bugs, request features, or track project progress.',
    href: `${GITHUB_URL}/issues`,
    external: true,
  },
  {
    title: 'License',
    description: 'MirDB is open source software released under the MIT License.',
    href: `${GITHUB_URL}/blob/master/LICENSE`,
    external: true,
  },
];
