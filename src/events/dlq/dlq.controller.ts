import { Controller, Get, HttpException, HttpStatus, Logger, Query } from "@nestjs/common";
import { DLQMessage, DLQStats } from "./dlq.interface.js";
import { DlqService } from "./dlq.service.js";

@Controller('dlq')
export class DlqController {
    private readonly logger = new Logger(DlqController.name);

    constructor(private readonly dlqService: DlqService) { }

    /**
     * GET /dlq/stats
     * Retorna estatísticas da DLQ
     */
    @Get('stats')
    async getStats(): Promise<DLQStats> {
        try {
            return await this.dlqService.getStats();
        } catch (error) {
            this.logger.error('Failed to get DLQ stats:', error);
            throw new HttpException(
                'Failed to get DLQ stats',
                HttpStatus.INTERNAL_SERVER_ERROR,
            );
        }
    }

    /**
     * GET /dlq/messages?limit=10
     * Lista mensagens na DLQ (sem remover)
     */
    @Get('messages')
    async getMessages(
        @Query('limit') limit?: string,
    ): Promise<{ count: number; messages: DLQMessage[] }> {
        try {
            const parsedLimit = limit ? parseInt(limit, 10) : 10;
            const messages = await this.dlqService.peekMessages(parsedLimit);

            return {
                count: messages.length,
                messages,
            };
        } catch (error) {
            this.logger.error('Failed to get DLQ messages:', error);
            throw new HttpException(
                'Failed to get DLQ messages',
                HttpStatus.INTERNAL_SERVER_ERROR,
            );
        }
    }
}