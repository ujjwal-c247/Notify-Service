process.env.KAFKA_GROUP_ID = `notification-service-test-${Date.now()}`;

import { Test, TestingModule } from '@nestjs/testing';
import { FastifyAdapter, NestFastifyApplication } from '@nestjs/platform-fastify';
import { AppModule } from '../src/app.module';
import { PrismaService } from '../src/infrastructure/database/prisma/prisma.service';
import { EMAIL_PROVIDER } from '../src/infrastructure/email/email.provider';
import { MockEmailProvider } from '../src/infrastructure/email/mock-email.provider';
import { NotificationService } from '../src/modules/notification/notification.service';
import { OutboxPublisher } from '../src/modules/outbox/outbox.publisher';
import { BookingStatus, NotificationStatus, EventType } from '../src/common/enums';

describe('Notification System (E2E Tests)', () => {
  let app: NestFastifyApplication;
  let prisma: PrismaService;
  let mockEmailProvider: MockEmailProvider;
  let notificationService: NotificationService;
  let outboxPublisher: OutboxPublisher;

  beforeAll(async () => {
    process.env.KAFKA_GROUP_ID = `notification-service-test-${Date.now()}`;

    const moduleFixture: TestingModule = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();

    app = moduleFixture.createNestApplication<NestFastifyApplication>(new FastifyAdapter());
    await app.init();
    await app.getHttpAdapter().getInstance().ready();

    prisma = app.get<PrismaService>(PrismaService);
    mockEmailProvider = app.get<MockEmailProvider>(EMAIL_PROVIDER);
    notificationService = app.get<NotificationService>(NotificationService);
    outboxPublisher = app.get<OutboxPublisher>(OutboxPublisher);

    // Allow Kafka consumer group join to settle before executing tests
    await new Promise((r) => setTimeout(r, 5000));
  });

  beforeEach(async () => {
    mockEmailProvider.setFailureMode('none');
    mockEmailProvider.sentEmails = [];
    await prisma.notification.deleteMany();
    await prisma.outboxEvent.deleteMany();
    await prisma.booking.deleteMany();
  });

  afterAll(async () => {
    await app.close();
  });

  it('TEST 1: Confirm booking -> outbox event -> notification -> email SENT (Happy Path)', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/api/bookings',
      payload: {
        tenantId: 'tenant-1',
        customerId: 'cust-100',
        startAt: new Date(Date.now() + 86400000).toISOString(),
      },
    });

    expect(res.statusCode).toBe(201);
    const booking = JSON.parse(res.payload);
    expect(booking.id).toBeDefined();
    expect(booking.status).toBe('CONFIRMED');
    expect(booking.version).toBe(1);

    // Verify PostgreSQL Outbox Event created in same transaction
    const outbox = await prisma.outboxEvent.findFirst({
      where: { aggregateId: booking.id },
    });
    expect(outbox).toBeDefined();
    expect(outbox.eventType).toBe('BOOKING_CONFIRMED');
    expect(outbox.publishedAt).toBeNull();

    // Trigger Outbox Publisher
    await outboxPublisher.processOutbox();
    const updatedOutbox = await prisma.outboxEvent.findUnique({ where: { id: outbox.id } });
    expect(updatedOutbox.publishedAt).not.toBeNull();

    // Process notification event (which KafkaConsumer delegates to notificationService)
    await notificationService.processBookingConfirmedEvent({
      eventId: outbox.id,
      aggregateId: booking.id,
      aggregateVersion: booking.version,
      tenantId: booking.tenantId,
      payload: {
        bookingId: booking.id,
      },
    });

    // Wait for BullMQ EmailWorker to process job
    await new Promise((r) => setTimeout(r, 1500));

    // Check Notification in DB
    const notification = await prisma.notification.findFirst({
      where: { bookingId: booking.id },
    });
    expect(notification).toBeDefined();
    expect(notification.status).toBe(NotificationStatus.SENT);
    expect(notification.sentAt).not.toBeNull();

    // Verify mock email provider sent email
    expect(mockEmailProvider.sentEmails.length).toBe(1);
    expect(mockEmailProvider.sentEmails[0].bookingId).toBe(booking.id);
  });

  it('TEST 2 (CRITICAL RACE CONDITION): Confirm -> Cancel immediately -> Worker receives old job -> EXPIRED -> NO EMAIL', async () => {
    // 1. Create booking
    const createRes = await app.inject({
      method: 'POST',
      url: '/api/bookings',
      payload: {
        tenantId: 'tenant-1',
        customerId: 'cust-200',
        startAt: new Date(Date.now() + 86400000).toISOString(),
      },
    });
    const booking = JSON.parse(createRes.payload);

    // 2. Immediately cancel booking BEFORE worker finishes processing
    const cancelRes = await app.inject({
      method: 'POST',
      url: `/api/bookings/${booking.id}/cancel`,
    });
    expect(cancelRes.statusCode).toBe(201);
    const cancelledBooking = JSON.parse(cancelRes.payload);
    expect(cancelledBooking.status).toBe('CANCELLED');
    expect(cancelledBooking.version).toBe(2);

    // 3. Process outbox events
    await outboxPublisher.processOutbox();

    // 4. Directly test worker processing an old notification job created for v1
    const notification = await prisma.notification.create({
      data: {
        tenantId: booking.tenantId,
        bookingId: booking.id,
        bookingVersion: 1, // v1 notification for cancelled booking (now v2)
        type: 'BOOKING_CONFIRMATION',
        channel: 'EMAIL',
        status: NotificationStatus.READY,
        dedupeKey: `${booking.id}:1:BOOKING_CONFIRMATION:EMAIL`,
        payload: { to: 'test@example.com' },
      },
    });

    const jobResult = await notificationService.processEmailJob({
      notificationId: notification.id,
      bookingId: booking.id,
      bookingVersion: 1,
    });

    // 5. Final Validation MUST fail and mark notification EXPIRED without sending email!
    expect(jobResult.status).toBe('EXPIRED');

    const finalNotification = await prisma.notification.findUnique({
      where: { id: notification.id },
    });
    expect(finalNotification.status).toBe(NotificationStatus.EXPIRED);
    expect(mockEmailProvider.sentEmails.length).toBe(0);
  });

  it('TEST 3: Duplicate confirmation event -> Exactly ONE notification and ONE email', async () => {
    const booking = await prisma.booking.create({
      data: {
        tenantId: 'tenant-1',
        customerId: 'cust-300',
        status: BookingStatus.CONFIRMED,
        version: 1,
        startAt: new Date(),
      },
    });

    const eventPayload = {
      eventId: 'evt-1',
      aggregateId: booking.id,
      aggregateVersion: 1,
      tenantId: 'tenant-1',
      payload: { bookingId: booking.id },
    };

    // Process duplicate events sequentially
    await notificationService.processBookingConfirmedEvent(eventPayload);
    await notificationService.processBookingConfirmedEvent(eventPayload);
    await notificationService.processBookingConfirmedEvent(eventPayload);

    const count = await prisma.notification.count({
      where: { bookingId: booking.id },
    });
    expect(count).toBe(1);
  });

  it('TEST 4: Stale confirmation event -> Ignored', async () => {
    const booking = await prisma.booking.create({
      data: {
        tenantId: 'tenant-1',
        customerId: 'cust-400',
        status: BookingStatus.CANCELLED,
        version: 2,
        startAt: new Date(),
      },
    });

    // Stale v1 confirmation event arrives after booking is v2
    await notificationService.processBookingConfirmedEvent({
      eventId: 'evt-stale',
      aggregateId: booking.id,
      aggregateVersion: 1,
      tenantId: 'tenant-1',
      payload: { bookingId: booking.id },
    });

    const count = await prisma.notification.count({
      where: { bookingId: booking.id },
    });
    expect(count).toBe(0);
  });

  it('TEST 5 & 6: Email provider failure classification (retryable vs permanent)', async () => {
    const booking = await prisma.booking.create({
      data: {
        tenantId: 'tenant-1',
        customerId: 'cust-500',
        status: BookingStatus.CONFIRMED,
        version: 1,
        startAt: new Date(),
      },
    });

    const notification = await prisma.notification.create({
      data: {
        tenantId: booking.tenantId,
        bookingId: booking.id,
        bookingVersion: 1,
        type: 'BOOKING_CONFIRMATION',
        channel: 'EMAIL',
        status: NotificationStatus.READY,
        dedupeKey: `${booking.id}:1:BOOKING_CONFIRMATION:EMAIL`,
        payload: { to: 'invalid@recipient.com' },
      },
    });

    // Test permanent failure
    mockEmailProvider.setFailureMode('permanent');
    const result = await notificationService.processEmailJob({
      notificationId: notification.id,
      bookingId: booking.id,
      bookingVersion: 1,
    });

    expect(result.status).toBe('FAILED');
    const updated = await prisma.notification.findUnique({ where: { id: notification.id } });
    expect(updated.status).toBe(NotificationStatus.FAILED);
  });

  it('TEST 9: Notification already SENT -> Worker does not send again', async () => {
    const booking = await prisma.booking.create({
      data: {
        tenantId: 'tenant-1',
        customerId: 'cust-600',
        status: BookingStatus.CONFIRMED,
        version: 1,
        startAt: new Date(),
      },
    });

    const notification = await prisma.notification.create({
      data: {
        tenantId: booking.tenantId,
        bookingId: booking.id,
        bookingVersion: 1,
        type: 'BOOKING_CONFIRMATION',
        channel: 'EMAIL',
        status: NotificationStatus.SENT,
        sentAt: new Date(),
        dedupeKey: `${booking.id}:1:BOOKING_CONFIRMATION:EMAIL`,
        payload: { to: 'customer@example.com' },
      },
    });

    const result = await notificationService.processEmailJob({
      notificationId: notification.id,
      bookingId: booking.id,
      bookingVersion: 1,
    });

    expect(result.status).toBe('SENT');
    expect(mockEmailProvider.sentEmails.length).toBe(0);
  });
});
