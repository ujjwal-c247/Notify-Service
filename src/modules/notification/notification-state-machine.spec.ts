import { NotificationStateMachine } from './notification-state-machine';
import { NotificationStatus } from '../../common/enums';
import { InvalidStateTransitionError } from '../../common/errors/domain.error';

describe('NotificationStateMachine', () => {
  it('should allow valid transitions', () => {
    expect(
      NotificationStateMachine.validateTransition(
        NotificationStatus.CREATED,
        NotificationStatus.READY,
      ),
    ).toBe(true);

    expect(
      NotificationStateMachine.validateTransition(
        NotificationStatus.READY,
        NotificationStatus.PROCESSING,
      ),
    ).toBe(true);

    expect(
      NotificationStateMachine.validateTransition(
        NotificationStatus.PROCESSING,
        NotificationStatus.SENT,
      ),
    ).toBe(true);

    expect(
      NotificationStateMachine.validateTransition(
        NotificationStatus.PROCESSING,
        NotificationStatus.EXPIRED,
      ),
    ).toBe(true);
  });

  it('should throw InvalidStateTransitionError on invalid transitions', () => {
    expect(() =>
      NotificationStateMachine.validateTransition(
        NotificationStatus.EXPIRED,
        NotificationStatus.READY,
      ),
    ).toThrow(InvalidStateTransitionError);

    expect(() =>
      NotificationStateMachine.validateTransition(
        NotificationStatus.SENT,
        NotificationStatus.READY,
      ),
    ).toThrow(InvalidStateTransitionError);

    expect(() =>
      NotificationStateMachine.validateTransition(
        NotificationStatus.DEAD_LETTER,
        NotificationStatus.READY,
      ),
    ).toThrow(InvalidStateTransitionError);
  });

  it('should identify terminal states correctly', () => {
    expect(NotificationStateMachine.isTerminal(NotificationStatus.SENT)).toBe(true);
    expect(NotificationStateMachine.isTerminal(NotificationStatus.EXPIRED)).toBe(true);
    expect(NotificationStateMachine.isTerminal(NotificationStatus.FAILED)).toBe(true);
    expect(NotificationStateMachine.isTerminal(NotificationStatus.READY)).toBe(false);
  });
});
