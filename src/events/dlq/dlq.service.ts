import { Injectable, Logger } from "@nestjs/common";
import { PaymentOrderMessage } from "../payment-queue.interface.js";
import { RabbitmqService } from "../rabbitmq/rabbitmq.service.js";
import { DLQMessage, DLQStats } from "./dlq.interface.js";

@Injectable()
export class DlqService {

    private readonly logger = new Logger(DlqService.name);

    private readonly DLQ_NAME = 'payment_queue.dlq';
    private readonly EXCHANGE = 'payments';
    private readonly ROUTING_KEY = 'payment.order';

    constructor(private readonly rabbitmqService: RabbitmqService) { }

    /**
     * Obtém estatísticas da DLQ
     */

    async getStats(): Promise<DLQStats> {

        const channel = this.rabbitmqService.getChannel();
        if (!channel) {
            throw new Error('RabbitMQ channel not available');
        }

        const queueInfo = await channel.checkQueue(this.DLQ_NAME);

        return {
            queueName: this.DLQ_NAME,
            messageCount: queueInfo.messageCount,
            consumerCount: queueInfo.consumerCount,
        };
    }

    /**
     * Obtém mensagens da DLQ sem removê-las (peek)
     * Usa o conceito de "get" com nack para visualizar sem consumir
     */
    async peekMessages(limit: number = 10): Promise<DLQMessage[]> {

        const channel = this.rabbitmqService.getChannel();
        if (!channel) {
            throw new Error('RabbitMQ channel not available');
        }

        const messages: DLQMessage[] = [];

        await channel.checkQueue(this.DLQ_NAME);

        for (let i = 0; i < limit; i++) {

            const msg = await channel.get(this.DLQ_NAME, { noAck: false });

            if (!msg) {
                break; // Não há mais mensagens
            }

            try {

                const content = JSON.parse(
                    msg.content.toString(),
                ) as PaymentOrderMessage;

                const xDeath = msg.properties.headers?.['x-death'] as
                    | Array<{
                        reason: string;
                        queue: string;
                        time: { getTime: () => number };
                        count: number;
                        exchange: string;
                        'routing-keys': string[];
                    }>
                    | undefined;

                const deathInfo = xDeath?.[0]
                    ? {
                        reason: xDeath[0].reason,
                        queue: xDeath[0].queue,
                        time: new Date(xDeath[0].time?.getTime?.() || Date.now()),
                        count: xDeath[0].count,
                        exchange: xDeath[0].exchange,
                        routingKeys: xDeath[0]['routing-keys'],
                    }
                    : undefined;

                const headers =
                    msg.properties.headers && typeof msg.properties.headers === 'object'
                        ? (msg.properties.headers as Record<string, unknown>)
                        : undefined;

                messages.push({
                    content,
                    properties: {
                        messageId: msg.properties.messageId as string | undefined,
                        timestamp: msg.properties.timestamp as number | undefined,
                        headers,
                    },
                    deathInfo,
                });

                channel.nack(msg, false, true);

            } catch (error) {

                channel.nack(msg, false, true);
                this.logger.error('Failed to parse DLQ message:', error);

            }
        }

        return messages;
    }
}