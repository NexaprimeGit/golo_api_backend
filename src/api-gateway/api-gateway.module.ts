import { Module, MiddlewareConsumer, NestModule, RequestMethod } from '@nestjs/common';
import { ApiGatewayController } from './api-gateway.controller';
import { ApiGatewayService } from './services/api-gateway.service';
import { ApiGatewayMiddleware } from './middleware/api-gateway.middleware';
import { RedisModule } from '../common/services/redis.module';

/**
 * API Gateway Module
 * 
 * Handles all incoming requests and provides:
 * - JWT Authentication
 * - Rate Limiting (Redis-backed)
 * - Request Logging
 * - Error Handling
 * - CORS Protection
 * 
 * All routes automatically pass through gateway middleware
 */
@Module({
  imports: [RedisModule],
  controllers: [ApiGatewayController],
  providers: [ApiGatewayService],
  exports: [ApiGatewayService]
})
export class ApiGatewayModule implements NestModule {
  configure(consumer: MiddlewareConsumer) {
    // Apply gateway middleware to ALL routes
    consumer
      .apply(ApiGatewayMiddleware)
      .forRoutes({ path: '*', method: RequestMethod.ALL });
  }
}
