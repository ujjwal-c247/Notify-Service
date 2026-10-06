import { Controller, Get, Param, NotFoundException } from '@nestjs/common';
import { NotificationService } from './notification.service';

@Controller('api/notifications')
export class NotificationController {
  constructor(private readonly notificationService: NotificationService) {}

  @Get(':id')
  async getNotification(@Param('id') id: string) {
    const notification = await this.notificationService.getNotification(id);
    if (!notification) {
      throw new NotFoundException(`Notification with ID ${id} not found`);
    }
    return notification;
  }
}
