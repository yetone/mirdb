import { PROJECT_STATUS } from '../../utils/constants';
import { StatusItem } from './StatusItem';
import styles from './ProjectStatus.module.css';

export function ProjectStatus() {
  return (
    <section
      id="project-status"
      className={styles.projectStatus}
      aria-labelledby="project-status-heading"
      role="region"
    >
      <h2 id="project-status-heading" className={styles.heading}>
        Project Status
      </h2>
      <ul className={styles.statusList} data-testid="project-status-list">
        {PROJECT_STATUS.map((item) => (
          <StatusItem key={item.title} item={item} />
        ))}
      </ul>
    </section>
  );
}
