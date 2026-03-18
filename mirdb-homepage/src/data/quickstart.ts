/**
 * Quick Start Steps Data.
 * Owner: Scenario 4 - Quick Start Section
 *
 * Contains setup steps:
 * 1. Clone repository
 * 2. Configure settings
 * 3. Run server
 */

import type { QuickStartStep } from '../types';

export const quickStartSteps: QuickStartStep[] = [
  {
    step: 1,
    title: 'Clone the Repository',
    code: 'git clone https://github.com/yetone/mirdb.git\ncd mirdb',
  },
  {
    step: 2,
    title: 'Configure',
    code: '# Edit config.toml to customize settings\n# Default configuration works out of the box\ncp config.example.toml config.toml',
  },
  {
    step: 3,
    title: 'Build and Run',
    code: 'cargo build --release\ncargo run --release',
  },
];

export const GITHUB_REPO_URL = 'https://github.com/yetone/mirdb';
