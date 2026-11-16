import { Module } from '@nestjs/common';
import { RabbitmqService } from './rabbitmq/rabbitmq.service.js';
import { PaymentQueueService } from './payment-queue/payment-queue.service.js';
import { ConfigModule } from '@nestjs/config';
import { PaymentConsumerService } from './payment-consumer/payment-consumer.service.js';
import { DlqService } from './dlq/dlq.service.js';
import { DlqController } from './dlq/dlq.controller.js';

@Module({
  imports: [ConfigModule],
  controllers: [DlqController],
  providers: [RabbitmqService, PaymentQueueService, PaymentConsumerService, DlqService]
})
export class EventsModule { }
