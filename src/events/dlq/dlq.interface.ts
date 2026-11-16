import { PaymentOrderMessage } from "../payment-queue.interface.js";

export interface DLQMessage {
    content: PaymentOrderMessage;
    properties: {
        messageId?: string;
        timestamp?: number;
        headers?: Record<string, unknown>;
    };
    deathInfo?: {
        reason: string;
        queue: string;
        time: Date;
        count: number;
        exchange: string;
        routingKeys: string[];
    };
}

export interface DLQStats {
    queueName: string;
    messageCount: number;
    consumerCount: number;
}