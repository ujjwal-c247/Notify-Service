import { NotificationType, NotificationChannel } from '../../common/enums';

export class NotificationPolicy {
  static generateDedupeKey(
    bookingId: string,
    bookingVersion: number,
    type: NotificationType,
    channel: NotificationChannel,
  ): string {
    return `${bookingId}:${bookingVersion}:${type}:${channel}`;
  }
}
