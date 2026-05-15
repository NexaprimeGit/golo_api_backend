import { Logger, Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { MongooseModule } from '@nestjs/mongoose';
import configuration from './config/configuration';
import { AnalyticsModule } from './analytics/analytics.module';
import { KafkaModule } from './kafka/kafka.module';
import { AdsModule } from './ads/ads.module';
import { UsersModule } from './users/users.module';
import { PaymentsModule } from './payments/payments.module';
import { ChatsModule } from './chats/chats.module';
import { CallsModule } from './calls/calls.module';
import { ReportsModule } from './reports/reports.module';
import { RedisModule } from './common/services/redis.module';
import { AuditLogsModule } from './audit-logs/audit-logs.module';
import { MerchantProductsModule } from './merchant-products/merchant-products.module';
import { MerchantDashboardModule } from './merchant-dashboard/merchant-dashboard.module';
import { BannersModule } from './banners/banners.module';
import { OffersModule } from './offers/offers.module';
// import { RecommendationsModule } from './recommendations/recommendations.module';
import { MerchantsModule } from './merchants/merchants.module';
import { OrdersModule } from './orders/orders.module';
import { VouchersModule } from './vouchers/vouchers.module';
import { ReviewsModule } from './reviews/reviews.module';
import { ApiGatewayModule } from './api-gateway/api-gateway.module';
import { AppController } from './app.controller';
import { AppService } from './app.service';

const logger = new Logger('MongoDB');

@Module({
  imports: [
    // Configuration - load environment variables
    ConfigModule.forRoot({
      isGlobal: true,
      load: [configuration],
      envFilePath: '.env',
    }),

    // MongoDB Connection
    MongooseModule.forRootAsync({
      imports: [ConfigModule],
      useFactory: async (configService: ConfigService) => {
        const uri = configService.get('config.mongodb.uri');
        return {
          uri: uri,
          connectionFactory: (connection) => {
            connection.on('connected', () => {
              logger.log('MongoDB connected successfully');
            });
            connection.on('error', (error) => {
              logger.error(`MongoDB connection error: ${error.message}`);
            });
            return connection;
          },
        };
      },
      inject: [ConfigService],
    }),

    // Feature Modules - order doesn't matter with forwardRef
    RedisModule, // Global Redis caching
    ApiGatewayModule, // API Gateway with auth, rate limiting, logging
    KafkaModule,
    AdsModule,
    UsersModule,
    MerchantsModule,
    PaymentsModule,
    ChatsModule,
    CallsModule,
    ReportsModule,
    AuditLogsModule,
    AnalyticsModule,
    MerchantProductsModule,
    MerchantDashboardModule,
    BannersModule,
    OffersModule,
    
    OrdersModule,
    VouchersModule,
    ReviewsModule,
  ],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule { }
