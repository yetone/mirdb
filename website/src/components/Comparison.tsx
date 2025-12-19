import './Comparison.css'

export interface ComparisonItem {
  feature: string
  mirdb: string
  memcached: string
  advantage: boolean
}

export interface ComparisonProps {
  title: string
  subtitle: string
  items: ComparisonItem[]
  persistenceHighlight: string
  compatibilityMessage: string
}

export function Comparison({
  title,
  subtitle,
  items,
  persistenceHighlight,
  compatibilityMessage,
}: ComparisonProps) {
  return (
    <section className="comparison" id="comparison">
      <div className="comparison-content">
        <h2 className="comparison-title">{title}</h2>
        <p className="comparison-subtitle">{subtitle}</p>

        <div className="comparison-highlight">
          <div className="highlight-card persistence">
            <span className="highlight-icon">&#128190;</span>
            <p className="highlight-text">{persistenceHighlight}</p>
          </div>
          <div className="highlight-card compatibility">
            <span className="highlight-icon">&#128260;</span>
            <p className="highlight-text">{compatibilityMessage}</p>
          </div>
        </div>

        <table className="comparison-table">
          <thead>
            <tr>
              <th>Feature</th>
              <th>MirDB</th>
              <th>Memcached</th>
            </tr>
          </thead>
          <tbody>
            {items.map((item, index) => (
              <tr key={index} className={item.advantage ? 'advantage' : ''}>
                <td>{item.feature}</td>
                <td className="mirdb-cell">{item.mirdb}</td>
                <td className="memcached-cell">{item.memcached}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  )
}
