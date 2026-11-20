import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import * as amqp from 'amqplib';

@Injectable()
export class RabbitmqService implements OnModuleInit, OnModuleDestroy {
    private readonly logger = new Logger(RabbitmqService.name);

    private connection: amqp.ChannelModel;
    private channel: amqp.Channel;

    constructor(private readonly configService: ConfigService) { }

    async onModuleInit() {
        await this.connect();
    }

    async onModuleDestroy() {
        await this.disconnect();
    }

    async waitForConnection(maxAttempts = 10, delayMs = 500): Promise<boolean> {
        for (let attempt = 1; attempt <= maxAttempts; attempt++) {
            if (this.channel) {
                return true;
            }
            this.logger.log(
                `⏳ Waiting for RabbitMQ connection... (attempt ${attempt}/${maxAttempts})`,
            );
            await new Promise((resolve) => setTimeout(resolve, delayMs));
        }
        return false;
    }

    private async connect() {
        try {
            this.connection = await amqp.connect(this.configService.get<string>('RABBITMQ_URL') || 'amqp://admin:admin@localhost:5672');
            this.channel = await this.connection.createChannel();
            this.logger.log('✅ Connected to RabbitMQ sucessfully');

            // Events listener para monitorar a conexão
            this.connection.on('error', (error) => {
                this.logger.error('❌ RabbitMQ connection error', error);
            });

            this.connection.on('close', () => {
                this.logger.warn('⚠️  RabbitMQ connection closed');
            });

            this.connection.on('blocked', (reason) => {
                this.logger.warn('⚠️  RabbitMQ connection blocked', reason);
            });

            this.connection.on('unblocked', () => {
                this.logger.log('✅ RabbitMQ connection unblocked');
            });
        } catch (error: Error | any) {
            this.logger.warn('⚠️  Failed to connect to RabbitMQ', error.message || error);
            throw error;
        }
    }

    private async disconnect() {
        try {
            if (this.channel) {
                await this.channel.close();
                this.logger.log('✅ RabbitMQ channel closed');
            }
            if (this.connection) {
                await this.connection.close();
                this.logger.log('✅ Disconnected from RabbitMQ successfully');
            }

        } catch (error: Error | any) {
            this.logger.error('❌ Failed to disconnect from RabbitMQ', error.message || error);
        }
    }

    getChannel(): amqp.Channel {
        return this.channel;
    }

    getConnection(): amqp.ChannelModel {
        return this.connection;
    }

    async publishMessage(exchange: string, routingKey: string, message: any): Promise<void> {
        try {
            if (!this.channel) {
                this.logger.warn('⚠️  RabbitMQ channel not available, skipping message publish');
                return;
            }

            await this.channel.assertExchange(exchange, 'topic', { durable: true });
            const messageBuffer = Buffer.from(JSON.stringify(message));

            const published = this.channel.publish(exchange, routingKey, messageBuffer, {
                persistent: true,
                timestamp: Date.now(),
                contentType: 'application/json',
            });

            if (!published) {
                throw new Error('❌ Failed to publish message to RabbitMQ');
            }

            this.logger.log(`✅ Message published to"${exchange}":"${routingKey}"`);
            this.logger.debug(`Message content: ${JSON.stringify(message)}`);


        } catch (error: Error | any) {
            this.logger.error('❌ Failed to publish message', error.message || error);
        }
    }

    async subscribeToQueue(queueName: string, exchange: string, routingKey: string, cb: (message: unknown) => Promise<void>, options: {
        maxRetries?: number,
        retryDelayMs?: number
    }): Promise<void> {
        try {

            const maxRetries = options.maxRetries ?? 3;
            const retryDelayMs = options.retryDelayMs ?? 3000;

            if (!this.channel) {
                this.logger.warn('⚠️  RabbitMQ channel not available, skipping queue subscription');
                return;
            }
            await this.channel.assertExchange(exchange, 'topic', { durable: true });

            const retryExchange = `${exchange}.retry.dlx`;
            await this.channel.assertExchange(retryExchange, 'topic', { durable: true });

            const dlxExchange = `${exchange}.dlx`;
            await this.channel.assertExchange(dlxExchange, 'topic', { durable: true });

            // DLQ

            const dlxName = `${queueName}.dlq`;
            const dlqRoutingKey = `${routingKey}.dead`;

            await this.channel.assertQueue(dlxName, { durable: true, arguments: { 'x-message-ttl': 604800000 } }); // 7 dias para análise de mensagens na DLQ

            await this.channel.bindQueue(dlxName, dlxExchange, dlqRoutingKey);

            // Retry

            const routingKeyRetry = `${routingKey}.retry`;

            const retryQueueName = `${queueName}.retry`;
            await this.channel.assertQueue(retryQueueName, { durable: true, arguments: { 'x-message-ttl': retryDelayMs, 'x-dead-letter-exchange': exchange, 'x-dead-letter-routing-key': routingKey } }); // 30 segundos de espera antes de reprocessar


            await this.channel.bindQueue(retryQueueName, retryExchange, routingKeyRetry);

            // Main Queue

            const queue = await this.channel.assertQueue(queueName, { durable: true, arguments: { 'x-message-ttl': 86400000, 'x-max-length': 10000, 'x-dead-letter-exchange': retryExchange, 'x-dead-letter-routing-key': routingKeyRetry } });

            await this.channel.bindQueue(queue.queue, exchange, routingKey);

            await this.channel.prefetch(1); // Process one message at a time

            this.channel.consume(queue.queue, async (msg) => {
                if (msg) {
                    try {
                        const message: unknown = JSON.parse(msg.content.toString());

                        this.logger.log(`📨 Message received from queue: ${queueName}`);
                        this.logger.debug(`Message content: ${JSON.stringify(message)}`);

                        await cb(message);

                        this.channel.ack(msg);

                        this.logger.log(
                            `✅ Message processed successfully from queue: ${queueName}`,
                        );

                    } catch (error: Error | any) {
                        this.logger.error('❌ Error processing message', error.message || error);
                        this.channel.nack(msg, false, false); // Reject the message and do not requeue
                        this.logger.warn(`⚠️  Message sent to dead-letter queue: ${dlxName}`);
                    }
                }
            });
        } catch (error: Error | any) {
            this.logger.error(`❌ Error subscribing to queue ${queueName}`, error.message || error);
        }
    }

    private getRetryCount(msg: amqp.Message): number {
        const xDeath = msg.properties.headers?.['x-death'] as | Array<{
            count: number,
            queue: string
        }> | undefined

        if (!xDeath || xDeath.length === 0) {
            return 0;
        }

        return xDeath.filter((death) => !death.queue.endsWith('.retry')).reduce((sum, death) => sum + (death.count || 0), 0);
    }
}


