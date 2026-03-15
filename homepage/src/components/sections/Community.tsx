/**
 * Community Section Component.
 * Owner: Scenario 12 - Contact and Community Section
 *
 * Responsibilities:
 * - GitHub issues link for bug reports
 * - Contribution guidelines
 * - Community engagement options
 *
 * Requirements:
 * - REQ-7: Contact/Community section
 */

import { MessageSquare, GitPullRequest, Users, Bug } from 'lucide-react'
import { ExternalLink } from '../ui/ExternalLink'
import { GITHUB_URL, GITHUB_ISSUES_URL } from '../../utils/constants'

interface CommunityCard {
  title: string
  description: string
  icon: 'bug' | 'pr' | 'discussions' | 'contribute'
  linkText: string
  href: string
}

const COMMUNITY_CARDS: CommunityCard[] = [
  {
    title: 'Report Issues',
    description: 'Found a bug or have a feature request? Open an issue on GitHub to let us know.',
    icon: 'bug',
    linkText: 'Open an Issue',
    href: GITHUB_ISSUES_URL,
  },
  {
    title: 'Contribute',
    description: 'We welcome contributions! Check out our contributing guidelines to get started.',
    icon: 'pr',
    linkText: 'Read Contributing Guide',
    href: `${GITHUB_URL}/blob/master/CONTRIBUTING.md`,
  },
  {
    title: 'Discussions',
    description: 'Have questions or ideas? Join the discussion with the MirDB community.',
    icon: 'discussions',
    linkText: 'Join Discussions',
    href: `${GITHUB_URL}/discussions`,
  },
  {
    title: 'Community',
    description: 'Connect with other users and contributors building with MirDB.',
    icon: 'contribute',
    linkText: 'View on GitHub',
    href: GITHUB_URL,
  },
]

const IconMap = {
  bug: Bug,
  pr: GitPullRequest,
  discussions: MessageSquare,
  contribute: Users,
}

export function Community() {
  return (
    <section
      id="community"
      className="py-16 sm:py-24 bg-white dark:bg-gray-900"
      aria-labelledby="community-heading"
      data-testid="community-section"
    >
      <div className="section-container">
        <div className="text-center max-w-3xl mx-auto mb-12">
          <h2
            id="community-heading"
            className="section-heading"
          >
            Join the Community
          </h2>
          <p className="section-description">
            MirDB is an open-source project and we'd love your help. Whether you're
            reporting bugs, suggesting features, or contributing code, there are many
            ways to get involved.
          </p>
        </div>

        <div
          className="grid grid-cols-1 md:grid-cols-2 gap-6"
          data-testid="community-grid"
        >
          {COMMUNITY_CARDS.map((card) => {
            const Icon = IconMap[card.icon]
            return (
              <div
                key={card.title}
                className="bg-gray-50 dark:bg-gray-800 rounded-xl p-6 transition-all hover:shadow-lg hover:scale-[1.02]"
                data-testid="community-card"
              >
                <div className="flex items-start gap-4">
                  <div
                    className="flex-shrink-0 w-12 h-12 bg-blue-100 dark:bg-blue-900 rounded-lg flex items-center justify-center"
                    data-testid="community-icon"
                  >
                    <Icon
                      className="w-6 h-6 text-blue-600 dark:text-blue-400"
                      aria-hidden="true"
                    />
                  </div>
                  <div className="flex-1">
                    <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-2">
                      {card.title}
                    </h3>
                    <p className="text-gray-600 dark:text-gray-300 mb-4 text-sm">
                      {card.description}
                    </p>
                    <ExternalLink
                      href={card.href}
                      className="text-blue-600 dark:text-blue-400 hover:text-blue-800 dark:hover:text-blue-300 font-medium text-sm"
                      aria-label={`${card.linkText} (opens in new tab)`}
                      data-testid={`community-link-${card.icon}`}
                    >
                      {card.linkText}
                    </ExternalLink>
                  </div>
                </div>
              </div>
            )
          })}
        </div>

        <div className="mt-12 text-center">
          <p className="text-gray-500 dark:text-gray-400 text-sm">
            MirDB is licensed under MIT. See the{' '}
            <ExternalLink
              href={`${GITHUB_URL}/blob/master/LICENSE`}
              className="text-blue-600 dark:text-blue-400 hover:underline"
              data-testid="license-link"
            >
              LICENSE
            </ExternalLink>{' '}
            file for details.
          </p>
        </div>
      </div>
    </section>
  )
}

export default Community
