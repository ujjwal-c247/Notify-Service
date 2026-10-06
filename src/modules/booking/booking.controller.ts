import { Controller, Post, Get, Body, Param } from '@nestjs/common';
import { BookingService } from './booking.service';
import { CreateBookingDto } from './dto/create-booking.dto';

@Controller('api/bookings')
export class BookingController {
  constructor(private readonly bookingService: BookingService) {}

  @Post()
  async createBooking(@Body() dto: CreateBookingDto) {
    return this.bookingService.createBooking(dto);
  }

  @Post(':id/cancel')
  async cancelBooking(@Param('id') id: string) {
    return this.bookingService.cancelBooking(id);
  }

  @Get(':id')
  async getBooking(@Param('id') id: string) {
    return this.bookingService.getBooking(id);
  }
}
