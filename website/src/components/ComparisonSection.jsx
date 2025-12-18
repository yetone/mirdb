import './ComparisonSection.css'

const CheckIcon = () => (
  <svg
    data-testid="checkmark-icon"
    aria-label="Advantage indicator"
    viewBox="0 0 24 24"
    width="20"
    height="20"
    fill="none"
    stroke="currentColor"
    strokeWidth="2.5"
    strokeLinecap="round"
    strokeLinejoin="round"
    className="checkmark"
  >
    <polyline points="20 6 9 17 4 12" />
  </svg>
)

const comparisonData = [
  {
    id: 'persistence',
    feature: 'Data Persistence',
    mirdb: 'Yes (SSTable)',
    memcached: 'No (Memory only)',
    mirdbAdvantage: true,
  },
  {
    id: 'protocol',
    feature: 'Protocol',
    mirdb: 'Memcached',
    memcached: 'Memcached',
    mirdbAdvantage: false,
  },
  {
    id: 'crash-recovery',
    feature: 'Crash Recovery',
    mirdb: 'WAL + SSTable',
    memcached: 'None',
    mirdbAdvantage: true,
  },
  {
    id: 'async-io',
    feature: 'Async I/O',
    mirdb: 'Tokio-based',
    memcached: 'Yes',
    mirdbAdvantage: true,
  },
]

const ComparisonSection = () => {
  return (
    <section className="comparison-section" id="comparison" data-testid="comparison-section">
      <h2 className="comparison-heading">MirDB vs Memcached</h2>
      <p className="comparison-subtitle">
        See how MirDB compares to standard memcached
      </p>
      <div className="comparison-table-wrapper">
        <table className="comparison-table" role="table">
          <thead>
            <tr>
              <th className="feature-header">Feature</th>
              <th className="mirdb-header">MirDB</th>
              <th className="memcached-header">Memcached</th>
            </tr>
          </thead>
          <tbody>
            {comparisonData.map((row) => (
              <tr key={row.id} data-testid={`comparison-row-${row.id}`}>
                <td className="feature-cell">{row.feature}</td>
                <td
                  className={`mirdb-cell ${row.mirdbAdvantage ? 'advantage' : ''}`}
                  data-testid={`mirdb-value-${row.id}`}
                  data-advantage={row.mirdbAdvantage}
                >
                  {row.mirdbAdvantage && (
                    <span className="advantage-indicator" data-testid="advantage-indicator">
                      <CheckIcon />
                    </span>
                  )}
                  <span className="cell-value">{row.mirdb}</span>
                </td>
                <td className="memcached-cell" data-testid={`memcached-value-${row.id}`}>
                  <span className="cell-value">{row.memcached}</span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  )
}

export default ComparisonSection
