import { Injectable, NotFoundException, Logger } from '@nestjs/common';
import { BookingRepository } from './booking.repository';
import { OutboxService } from '../outbox/outbox.service';
import { CreateBookingDto } from './dto/create-booking.dto';
import { EventType, AggregateType } from '../../common/enums';

@Injectable()
export class BookingService {
  private readonly logger = new Logger(BookingService.name);

  constructor(
    private readonly bookingRepository: BookingRepository,
    private readonly outboxService: OutboxService,
  ) {}

  async createBooking(dto: CreateBookingDto) {
    const booking = await this.bookingRepository.createWithOutbox(
      {
        tenantId: dto.tenantId,
        customerId: dto.customerId,
        startAt: new Date(dto.startAt),
      },
      async (tx, newBooking) => {
        await this.outboxService.createOutboxEvent(tx, {
          eventType: EventType.BOOKING_CONFIRMED,
          aggregateType: AggregateType.BOOKING,
          aggregateId: newBooking.id,
          aggregateVersion: newBooking.version,
          payload: {
            eventId: undefined, // Kafka client can generate or use outbox id
            bookingId: newBooking.id,
            tenantId: newBooking.tenantId,
            customerId: newBooking.customerId,
            status: newBooking.status,
            version: newBooking.version,
            startAt: newBooking.startAt.toISOString(),
          },
        });
      },
    );

    this.logger.log({
      event: 'booking.created',
      bookingId: booking.id,
      tenantId: booking.tenantId,
      version: booking.version,
    });

    return booking;
  }

  async cancelBooking(id: string) {
    const existing = await this.bookingRepository.findById(id);
    if (!existing) {
      throw new NotFoundException(`Booking with ID ${id} not found`);
    }

    const booking = await this.bookingRepository.cancelWithOutbox(
      id,
      async (tx, updatedBooking) => {
        await this.outboxService.createOutboxEvent(tx, {
          eventType: EventType.BOOKING_CANCELLED,
          aggregateType: AggregateType.BOOKING,
          aggregateId: updatedBooking.id,
          aggregateVersion: updatedBooking.version,
          payload: {
            bookingId: updatedBooking.id,
            tenantId: updatedBooking.tenantId,
            customerId: updatedBooking.customerId,
            status: updatedBooking.status,
            version: updatedBooking.version,
          },
        });
      },
    );

    this.logger.log({
      event: 'booking.cancelled',
      bookingId: booking.id,
      tenantId: booking.tenantId,
      version: booking.version,
    });

    return booking;
  }

  async getBooking(id: string) {
    const booking = await this.bookingRepository.findById(id);
    if (!booking) {
      throw new NotFoundException(`Booking with ID ${id} not found`);
    }
    return booking;
  }
}
