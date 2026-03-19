/**
 * Project status data for MirDB Homepage.
 * Owner: Scenario 4 - Project Status Section
 */

import type { ProjectFeature } from '@/types';

export const implementedFeatures: ProjectFeature[] = [
  {
    name: 'Async networking with Tokio',
    implemented: true,
  },
  {
    name: 'Memtable with skip list',
    implemented: true,
  },
  {
    name: 'Minor compaction',
    implemented: true,
  },
  {
    name: 'Major compaction',
    implemented: true,
  },
];

export const plannedFeatures: ProjectFeature[] = [
  {
    name: 'Raft consensus',
    implemented: false,
  },
];

export const CI_PIPELINE_URL = 'https://circleci.com/gh/yetone/mirdb';
