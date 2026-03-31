/**
 * Resources Section Component
 * Owner: Scenario 9 - Resource Links and Footer
 *
 * Displays links to project resources:
 * - GitHub repository
 * - Documentation
 * - Community links
 *
 * Requirements: REQ-4, Story 5 (Resource Navigation)
 */

import { Github, BookOpen, Users, ExternalLink } from 'lucide-react';
import { GITHUB_URL } from '../../constants/content';

interface ResourceLink {
  id: string;
  title: string;
  description: string;
  href: string;
  icon: React.ReactNode;
}

const resources: ResourceLink[] = [
  {
    id: 'github',
    title: 'GitHub Repository',
    description: 'View source code, report issues, and contribute to MirDB.',
    href: GITHUB_URL,
    icon: <Github className="w-8 h-8" aria-hidden="true" />,
  },
  {
    id: 'documentation',
    title: 'Documentation',
    description: 'Learn how to install, configure, and use MirDB effectively.',
    href: `${GITHUB_URL}#readme`,
    icon: <BookOpen className="w-8 h-8" aria-hidden="true" />,
  },
  {
    id: 'community',
    title: 'Community',
    description: 'Join discussions, ask questions, and connect with other users.',
    href: `${GITHUB_URL}/discussions`,
    icon: <Users className="w-8 h-8" aria-hidden="true" />,
  },
];

export function ResourcesSection() {
  return (
    <section
      id="resources"
      className="py-16 px-4 sm:px-6 lg:px-8 bg-gray-50 dark:bg-gray-800"
      aria-labelledby="resources-heading"
      data-testid="resources-section"
    >
      <div className="max-w-6xl mx-auto">
        <div className="text-center mb-12">
          <h2
            id="resources-heading"
            className="text-3xl font-bold text-gray-900 dark:text-white sm:text-4xl"
            data-testid="resources-heading"
          >
            Resources
          </h2>
          <p className="mt-4 text-lg text-gray-600 dark:text-gray-300">
            Everything you need to get started with MirDB
          </p>
        </div>

        <div
          className="grid grid-cols-1 md:grid-cols-3 gap-8"
          data-testid="resources-grid"
        >
          {resources.map((resource) => (
            <a
              key={resource.id}
              href={resource.href}
              target="_blank"
              rel="noopener noreferrer"
              className="group flex flex-col items-center p-6 bg-white dark:bg-gray-900 rounded-lg shadow-md hover:shadow-lg transition-shadow border border-gray-200 dark:border-gray-700"
              data-testid={`resource-link-${resource.id}`}
            >
              <div className="text-primary-600 dark:text-primary-400 mb-4 group-hover:scale-110 transition-transform">
                {resource.icon}
              </div>
              <h3 className="text-xl font-semibold text-gray-900 dark:text-white mb-2 flex items-center">
                {resource.title}
                <ExternalLink className="w-4 h-4 ml-2 opacity-0 group-hover:opacity-100 transition-opacity" aria-hidden="true" />
              </h3>
              <p className="text-gray-600 dark:text-gray-300 text-center">
                {resource.description}
              </p>
            </a>
          ))}
        </div>
      </div>
    </section>
  );
}
