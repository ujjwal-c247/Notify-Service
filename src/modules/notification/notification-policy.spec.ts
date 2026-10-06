import { NotificationPolicy } from './notification-policy';
import { NotificationType, NotificationChannel } from '../../common/enums';

describe('NotificationPolicy', () => {
  it('should generate deterministic dedupe keys', () => {
    const key = NotificationPolicy.generateDedupeKey(
      'B123',
      1,
      NotificationType.BOOKING_CONFIRMATION,
      NotificationChannel.EMAIL,
    );

    expect(key).toBe('B123:1:BOOKING_CONFIRMATION:EMAIL');
  });
});
