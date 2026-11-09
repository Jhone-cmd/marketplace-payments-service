import { Module } from '@nestjs/common';
import { RabbitmqService } from './rabbitmq/rabbitmq.service.js';

@Module({
  providers: [RabbitmqService]
})
export class EventsModule {}
