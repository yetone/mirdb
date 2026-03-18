/**
 * Status Badges Section.
 * Owner: Scenario 5 - Status Badges and Project Health
 *
 * Displays:
 * - CircleCI build status badge
 * - Optional: GitHub stars, version badge
 *
 * Expected exports:
 * - StatusBadges: React.FC
 */

import { Badge } from '../ui/Badge';
import { CIRCLECI_BADGE_URL, GITHUB_URL } from '../../utils/constants';

// CircleCI SVG badge URL for the mirdb project
const CIRCLECI_BADGE_IMAGE_URL = 'https://circleci.com/gh/yetone/mirdb.svg?style=svg';

export function StatusBadges() {
  return (
    <section
      id="status"
      className="py-12 bg-slate-50 dark:bg-slate-900"
      aria-label="Project Status Badges"
    >
      <div className="max-w-4xl mx-auto px-4 text-center">
        <h2 className="text-2xl font-semibold text-slate-800 dark:text-slate-200 mb-6">
          Project Health
        </h2>

        <div className="flex flex-wrap justify-center gap-4" data-testid="badges-container">
          {/* CircleCI Build Status Badge */}
          <Badge
            imageUrl={CIRCLECI_BADGE_IMAGE_URL}
            linkUrl={CIRCLECI_BADGE_URL}
            alt="CircleCI Build Status"
          />

          {/* GitHub Stars Badge */}
          <Badge
            imageUrl={`https://img.shields.io/github/stars/yetone/mirdb?style=social`}
            linkUrl={GITHUB_URL}
            alt="GitHub Stars"
          />

          {/* License Badge */}
          <Badge
            imageUrl="https://img.shields.io/badge/license-MIT-blue.svg"
            linkUrl={`${GITHUB_URL}/blob/master/LICENSE`}
            alt="MIT License"
          />
        </div>
      </div>
    </section>
  );
}
