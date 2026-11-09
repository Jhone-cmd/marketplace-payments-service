import { Module } from '@nestjs/common';
import { RabbitmqService } from './rabbitmq/rabbitmq.service.js';
import { PaymentQueueService } from './payment-queue/payment-queue.service.js';
import { ConfigModule } from '@nestjs/config';

@Module({
  imports: [ConfigModule],
  providers: [RabbitmqService, PaymentQueueService]
})
export class EventsModule { }
