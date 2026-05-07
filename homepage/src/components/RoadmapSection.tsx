import { ROADMAP_ITEMS } from '@/lib/constants';

export default function RoadmapSection() {
  const implemented = ROADMAP_ITEMS.filter((item) => item.status === 'implemented');
  const planned = ROADMAP_ITEMS.filter((item) => item.status === 'planned');

  return (
    <section
      id="roadmap"
      className="py-16 px-4 sm:px-6 lg:px-8"
      data-testid="roadmap-section"
    >
      <div className="max-w-4xl mx-auto">
        <h2 className="text-3xl font-bold text-center mb-12">Roadmap</h2>

        <div className="grid md:grid-cols-2 gap-12">
          {/* Implemented Features */}
          <div>
            <h3 className="text-xl font-semibold mb-6 flex items-center gap-2">
              <span className="inline-flex items-center justify-center w-6 h-6 rounded-full bg-green-500 text-white text-sm">
                <svg
                  xmlns="http://www.w3.org/2000/svg"
                  className="w-4 h-4"
                  viewBox="0 0 20 20"
                  fill="currentColor"
                  aria-hidden="true"
                >
                  <path
                    fillRule="evenodd"
                    d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z"
                    clipRule="evenodd"
                  />
                </svg>
              </span>
              Implemented
            </h3>
            <ul className="space-y-4" data-testid="implemented-list">
              {implemented.map((item) => (
                <li
                  key={item.title}
                  className="flex items-start gap-3 p-4 rounded-lg bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800"
                  data-testid="implemented-item"
                >
                  <span
                    className="inline-flex items-center justify-center w-5 h-5 mt-0.5 rounded-full bg-green-500 text-white flex-shrink-0"
                    aria-hidden="true"
                  >
                    <svg
                      xmlns="http://www.w3.org/2000/svg"
                      className="w-3 h-3"
                      viewBox="0 0 20 20"
                      fill="currentColor"
                    >
                      <path
                        fillRule="evenodd"
                        d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z"
                        clipRule="evenodd"
                      />
                    </svg>
                  </span>
                  <span className="text-gray-900 dark:text-gray-100">{item.title}</span>
                </li>
              ))}
            </ul>
          </div>

          {/* Planned Features */}
          <div>
            <h3 className="text-xl font-semibold mb-6 flex items-center gap-2">
              <span className="inline-flex items-center justify-center w-6 h-6 rounded-full bg-amber-500 text-white text-sm">
                <svg
                  xmlns="http://www.w3.org/2000/svg"
                  className="w-4 h-4"
                  viewBox="0 0 20 20"
                  fill="currentColor"
                  aria-hidden="true"
                >
                  <path
                    fillRule="evenodd"
                    d="M10 18a8 8 0 100-16 8 8 0 000 16zm1-12a1 1 0 10-2 0v4a1 1 0 00.293.707l2.828 2.829a1 1 0 101.415-1.415L11 9.586V6z"
                    clipRule="evenodd"
                  />
                </svg>
              </span>
              Planned
            </h3>
            <ul className="space-y-4" data-testid="planned-list">
              {planned.map((item) => (
                <li
                  key={item.title}
                  className="flex items-start gap-3 p-4 rounded-lg bg-amber-50 dark:bg-amber-900/20 border border-amber-200 dark:border-amber-800"
                  data-testid="planned-item"
                >
                  <span
                    className="inline-flex items-center justify-center w-5 h-5 mt-0.5 rounded-full bg-amber-500 text-white flex-shrink-0"
                    aria-hidden="true"
                  >
                    <svg
                      xmlns="http://www.w3.org/2000/svg"
                      className="w-3 h-3"
                      viewBox="0 0 20 20"
                      fill="currentColor"
                    >
                      <path
                        fillRule="evenodd"
                        d="M10 18a8 8 0 100-16 8 8 0 000 16zm1-12a1 1 0 10-2 0v4a1 1 0 00.293.707l2.828 2.829a1 1 0 101.415-1.415L11 9.586V6z"
                        clipRule="evenodd"
                      />
                    </svg>
                  </span>
                  <span className="text-gray-700 dark:text-gray-300">{item.title}</span>
                </li>
              ))}
            </ul>
          </div>
        </div>
      </div>
    </section>
  );
}
