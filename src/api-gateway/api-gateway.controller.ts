import { Controller, Get, HttpCode, HttpStatus } from '@nestjs/common';
import { ApiGatewayService } from './services/api-gateway.service';

@Controller('api')
export class ApiGatewayController {
  constructor(private gatewayService: ApiGatewayService) {}

  /**
   * Health check endpoint for gateway
   * This allows monitoring if the gateway and all dependencies are working
   */
  @Get('health')
  @HttpCode(HttpStatus.OK)
  async getHealth() {
    return await this.gatewayService.getGatewayHealth();
  }

  /**
   * Gateway status - all routes pass through here
   * This endpoint should never be called directly (all routes are handled by middleware)
   * But it serves as confirmation that gateway is working
   */
  @Get('gateway/status')
  @HttpCode(HttpStatus.OK)
  async getGatewayStatus() {
    return {
      status: 'Gateway is operational',
      message: 'All requests are routed through API Gateway middleware',
      timestamp: new Date().toISOString(),
      features: [
        'JWT Authentication',
        'Rate Limiting',
        'Request Logging',
        'Error Handling',
        'CORS Protection'
      ]
    };
  }
}
