import { Injectable } from '@nestjs/common';
import { PrismaService } from '../../infrastructure/database/prisma/prisma.service';
import { BookingStatus, Prisma } from '@prisma/client';

@Injectable()
export class BookingRepository {
  constructor(private readonly prisma: PrismaService) {}

  async findById(id: string, tx?: Prisma.TransactionClient) {
    const client = tx || this.prisma;
    return client.booking.findUnique({
      where: { id },
    });
  }

  async createWithOutbox(
    data: { tenantId: string; customerId: string; startAt: Date },
    createOutbox: (tx: Prisma.TransactionClient, booking: any) => Promise<any>,
  ) {
    return this.prisma.$transaction(async (tx) => {
      const booking = await tx.booking.create({
        data: {
          tenantId: data.tenantId,
          customerId: data.customerId,
          startAt: data.startAt,
          status: BookingStatus.CONFIRMED,
          version: 1,
        },
      });

      await createOutbox(tx, booking);

      return booking;
    });
  }

  async cancelWithOutbox(
    id: string,
    createOutbox: (tx: Prisma.TransactionClient, booking: any) => Promise<any>,
  ) {
    return this.prisma.$transaction(async (tx) => {
      const existing = await tx.booking.findUnique({
        where: { id },
      });

      if (!existing) {
        return null;
      }

      if (existing.status === BookingStatus.CANCELLED) {
        return existing;
      }

      const updated = await tx.booking.update({
        where: { id },
        data: {
          status: BookingStatus.CANCELLED,
          version: { increment: 1 },
        },
      });

      await createOutbox(tx, updated);

      return updated;
    });
  }
}
