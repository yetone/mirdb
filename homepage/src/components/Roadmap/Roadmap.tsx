/**
 * Roadmap Section Component
 * Owner: Scenario 7 - Planned Features Section
 *
 * Displays project status and roadmap:
 * - Completed features (with checkmarks)
 * - Planned features (Raft consensus)
 * - Visual distinction between states
 */
import { ROADMAP_ITEMS } from '../../utils/constants';
import { RoadmapItem } from './RoadmapItem';
import './Roadmap.css';

export function Roadmap() {
  const completedItems = ROADMAP_ITEMS.filter(item => item.status === 'completed');
  const plannedItems = ROADMAP_ITEMS.filter(item => item.status === 'planned');

  return (
    <section
      id="roadmap"
      className="section roadmap-section"
      aria-labelledby="roadmap-heading"
    >
      <div className="container">
        <h2 id="roadmap-heading" className="roadmap-title">
          Project Status
        </h2>
        <p className="roadmap-subtitle">
          Track our progress and see what's coming next
        </p>

        <div className="roadmap-columns">
          {/* Completed Features */}
          <div className="roadmap-column roadmap-column--completed">
            <h3 className="roadmap-column-title">
              <span className="roadmap-column-icon" aria-hidden="true">✓</span>
              Completed
            </h3>
            <ul className="roadmap-list" data-testid="completed-features">
              {completedItems.map((item, index) => (
                <RoadmapItem key={index} item={item} />
              ))}
            </ul>
          </div>

          {/* Planned Features */}
          <div className="roadmap-column roadmap-column--planned">
            <h3 className="roadmap-column-title">
              <span className="roadmap-column-icon" aria-hidden="true">◎</span>
              Planned
            </h3>
            <ul className="roadmap-list" data-testid="planned-features">
              {plannedItems.map((item, index) => (
                <RoadmapItem key={index} item={item} />
              ))}
            </ul>
          </div>
        </div>
      </div>
    </section>
  );
}
