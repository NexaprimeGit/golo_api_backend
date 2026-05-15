import { Injectable, NestMiddleware, ForbiddenException } from '@nestjs/common';
import { Request, Response, NextFunction } from 'express';
import { ApiGatewayService } from '../services/api-gateway.service';
import { getAccessTokenFromRequest } from '../../common/utils/auth-token.util';

@Injectable()
export class ApiGatewayMiddleware implements NestMiddleware {
  constructor(private gatewayService: ApiGatewayService) {}

  async use(req: Request, res: Response, next: NextFunction) {
    const startTime = Date.now();
    let path = req.path;
    const method = req.method;
    const ip = req.ip;
    // Log incoming path
    console.log(`[Gateway] Incoming request: ${method} ${path}`);

    // Add request ID
    const requestId = `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
    req['id'] = requestId;
    res.set('X-Request-ID', requestId);

    try {
      // Check if public route - try without /api prefix first, then with
      let isPublic = this.gatewayService.isPublicRoute(path);
      
      // If path includes /api, also try without it
      if (!isPublic && path.startsWith('/api')) {
        const pathWithoutApi = path.substring(4); // Remove /api
        isPublic = this.gatewayService.isPublicRoute(pathWithoutApi);
        if (isPublic) {
          path = pathWithoutApi; // Use the path without /api
          console.log(`[Gateway] Matched as public (without /api prefix): ${path}`);
        }
      }

      // Extract and validate token if not public
      let userContext: any = null;
      
      if (!isPublic) {
        const token = getAccessTokenFromRequest(req);

        if (!token) {
          console.log(`[Gateway] ❌ AUTH FAILED: ${method} ${path} - Missing token`);
          throw new ForbiddenException('Missing or invalid authorization token');
        }

        userContext = this.gatewayService.validateToken(token);

        if (!userContext) {
          console.log(`[Gateway] ❌ AUTH FAILED: ${method} ${path} - Invalid token`);
          throw new ForbiddenException('Invalid or expired token');
        }
        
        console.log(`[Gateway] ✅ AUTH PASSED: ${method} ${path} for user ${userContext.userId}`);
      }

      // Store user context in request
      if (userContext) {
        req['user'] = {
          userId: userContext.userId,
          role: userContext.role
        };
      }

      // Log response
      const originalSend = res.send;
      const gatewayService = this.gatewayService;
      res.send = function(data) {
        const duration = Date.now() - startTime;
        
        const logContext = {
          userId: userContext?.userId || 'anonymous',
          path,
          method,
          ip
        };

        gatewayService.logRequest(logContext, res.statusCode, duration);

        return originalSend.call(this, data);
      };

      next();
    } catch (error) {
      const duration = Date.now() - startTime;
      
      if (error instanceof ForbiddenException) {
        return res.status(403).json({
          statusCode: 403,
          message: error.message,
          error: 'Forbidden',
          requestId
        });
      }

      return res.status(500).json({
        statusCode: 500,
        message: 'Gateway error',
        error: process.env.NODE_ENV === 'production' ? 'Internal Server Error' : error.message,
        requestId
      });
    }
  }
}
