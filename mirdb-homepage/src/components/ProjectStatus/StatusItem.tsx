import type { ProjectStatusItem } from '../../types';
import styles from './ProjectStatus.module.css';

interface StatusItemProps {
  item: ProjectStatusItem;
}

export function StatusItem({ item }: StatusItemProps) {
  const { title, completed } = item;

  return (
    <li
      className={`${styles.statusItem} ${completed ? styles.completed : styles.planned}`}
      data-testid="status-item"
    >
      <span
        className={styles.statusIcon}
        aria-label={completed ? 'Completed' : 'Planned'}
        role="img"
      >
        {completed ? '✓' : '○'}
      </span>
      <span className={styles.statusTitle}>{title}</span>
    </li>
  );
}
