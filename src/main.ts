import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { AppModule } from './app.module';
import { Logger, ValidationPipe } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { json, urlencoded } from 'express';

const parseBoolean = (value?: string): boolean => {
  if (!value) return false;
  return ['1', 'true', 'yes', 'on'].includes(value.toLowerCase());
};

const validationPipe = new ValidationPipe({
  whitelist: true,
  forbidNonWhitelisted: true,
  transform: true,
  transformOptions: {
    enableImplicitConversion: true,
  },
});

async function bootstrap() {
  const logger = new Logger('Bootstrap');
  const kafkaEnabled = parseBoolean(process.env.ENABLE_KAFKA);
  const kafkaStrict = parseBoolean(process.env.KAFKA_STRICT ?? 'false');

  const app = await NestFactory.create(AppModule, { rawBody: true });
  const configService = app.get(ConfigService);

  // Allow larger payloads for banner image submissions (base64 data URLs).
  app.use(json({ limit: '15mb' }));
  app.use(urlencoded({ extended: true, limit: '15mb' }));

  app.useGlobalPipes(validationPipe);

  if (kafkaEnabled) {
    const brokers = (process.env.KAFKA_BROKERS || '')
      .split(',')
      .map((item) => item.trim())
      .filter(Boolean);

    if (brokers.length === 0) {
      const message = 'ENABLE_KAFKA=true but KAFKA_BROKERS is empty. Set KAFKA_BROKERS in .env.';
      if (kafkaStrict) {
        throw new Error(message);
      }
      logger.warn(`${message} Starting HTTP server without Kafka.`);
    } else {
      try {
        app.connectMicroservice<MicroserviceOptions>({
          transport: Transport.KAFKA,
          options: {
            client: {
              clientId: process.env.KAFKA_CLIENT_ID || 'golo-backend',
              brokers,
            },
            consumer: {
              groupId: process.env.KAFKA_GROUP_ID || 'golo-consumer-group',
            },
            producer: {
              allowAutoTopicCreation: true,
            },
          },
        });

        await app.startAllMicroservices();
        logger.log(`Kafka mode enabled. Brokers: ${brokers.join(', ')}`);
      } catch (error) {
        const errorMessage = error instanceof Error ? error.message : String(error);
        const message = `Kafka startup failed for brokers ${brokers.join(', ')}: ${errorMessage}`;
        if (kafkaStrict) {
          throw new Error(message);
        }
        logger.warn(`${message}. Starting HTTP server without Kafka.`);
      }
    }
  }

  const corsOrigins = configService.get<string[]>('config.cors.origins') || [];

  app.enableCors({
    origin: corsOrigins.length > 0 ? corsOrigins : ['http://localhost:3000', 'http://localhost:3001'],
    credentials: true,
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'PATCH'],
    allowedHeaders: ['Content-Type', 'Authorization'],
  });

  // Set global API prefix
  app.setGlobalPrefix('api');

  const port = configService.get('config.service.port');
  await app.listen(port);
  logger.log(`HTTP mode enabled. Ads microservice is running on port ${port}`);
}
bootstrap();
