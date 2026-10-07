import { NotificationStatus } from '../../common/enums';
import { InvalidStateTransitionError } from '../../common/errors/domain.error';

const ALLOWED_TRANSITIONS: Record<NotificationStatus, NotificationStatus[]> = {
  [NotificationStatus.CREATED]: [
    NotificationStatus.READY,
    NotificationStatus.EXPIRED,
    NotificationStatus.SUPPRESSED,
  ],
  [NotificationStatus.READY]: [
    NotificationStatus.PROCESSING,
    NotificationStatus.SENT,
    NotificationStatus.FAILED,
    NotificationStatus.RETRYING,
    NotificationStatus.EXPIRED,
    NotificationStatus.SUPPRESSED,
  ],
  [NotificationStatus.PROCESSING]: [
    NotificationStatus.SENT,
    NotificationStatus.RETRYING,
    NotificationStatus.FAILED,
    NotificationStatus.EXPIRED,
    NotificationStatus.SUPPRESSED,
  ],
  [NotificationStatus.RETRYING]: [
    NotificationStatus.PROCESSING,
    NotificationStatus.DEAD_LETTER,
    NotificationStatus.FAILED,
    NotificationStatus.EXPIRED,
    NotificationStatus.SUPPRESSED,
  ],
  [NotificationStatus.SENT]: [],
  [NotificationStatus.FAILED]: [],
  [NotificationStatus.EXPIRED]: [],
  [NotificationStatus.SUPPRESSED]: [],
  [NotificationStatus.DEAD_LETTER]: [],
};

export class NotificationStateMachine {
  static validateTransition(from: NotificationStatus, to: NotificationStatus): boolean {
    if (from === to) return true;
    const allowed = ALLOWED_TRANSITIONS[from] || [];
    if (!allowed.includes(to)) {
      throw new InvalidStateTransitionError(from, to);
    }
    return true;
  }

  static isTerminal(status: NotificationStatus): boolean {
    return ([
      NotificationStatus.SENT,
      NotificationStatus.FAILED,
      NotificationStatus.EXPIRED,
      NotificationStatus.SUPPRESSED,
      NotificationStatus.DEAD_LETTER,
    ] as NotificationStatus[]).includes(status);
  }
}
