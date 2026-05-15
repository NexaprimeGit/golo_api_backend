import { Injectable } from '@nestjs/common';
import { RedisService } from '../../common/services/redis.service';
import * as jwt from 'jsonwebtoken';

export interface RateLimitConfig {
  windowMs: number;  // Time window in milliseconds
  maxRequests: number;  // Max requests per window
}

export interface RequestContext {
  userId?: string;
  role?: string;
  token?: string;
  ip?: string;
  path?: string;
  method?: string;
}

@Injectable()
export class ApiGatewayService {
  private readonly rateLimitConfigs: Map<string, RateLimitConfig>;
  private readonly jwtSecret: string;
  private readonly redisKeyPrefix: string = 'golo-gateway:';

  constructor(private redisService: RedisService) {
    this.jwtSecret = process.env.JWT_SECRET || 'supersecret123';
    
    // Rate limit configurations
    this.rateLimitConfigs = new Map([
      ['default', { windowMs: 60000, maxRequests: 100 }],        // 100 per minute
      ['/api/payments', { windowMs: 60000, maxRequests: 50 }],   // 50 per minute
      ['/api/users/change-password', { windowMs: 60000, maxRequests: 5 }],
    ]);
  }

  /**
   * Validate JWT token
   */
  validateToken(token: string): RequestContext | null {
    try {
      const decoded = jwt.verify(token, this.jwtSecret) as any;
      return {
        userId: decoded.userId || decoded.sub,
        role: decoded.role,
        token: token
      };
    } catch (error) {
      return null;
    }
  }

  /**
   * Check if route is public (no auth required)
   * Handles paths with or without /api prefix
   */
  isPublicRoute(inputPath: string): boolean {
    // Normalize path
    let path = inputPath || '';
    if (!path.startsWith('/')) {
      path = '/' + path;
    }
    
    // Remove /api prefix if present for comparison
    let pathForComparison = path;
    if (path.startsWith('/api')) {
      pathForComparison = path.substring(4);
    }

    const publicRoutes = [
      // Authentication endpoints
      '/auth/login',
      '/auth/register',
      '/auth/refresh-token',
      '/auth/forgot-password',
      '/auth/social-auth',
      
      // User endpoints (public profile access)
      '/users/login',
      '/users/register',
      '/users/social-auth',
      '/users/refresh',
      '/users/',  // Public user profiles
      
      // Merchants - public endpoints
      '/merchants/public',
      '/merchants/login',
      '/merchants/register',
      '/merchant/login',
      '/merchant/register',
      '/merchants/search',
      '/merchants/',  // Public merchant profiles
      
      // Ads/Listings - search and browse
      '/ads/search',
      '/ads/list',
      '/ads/',  // Get single ad
      
      // Offers - public endpoints
      '/offers/list',
      '/offers/search',
      '/offers/',  // Get single offer
      
      // Products - search and browse
      '/products/search',
      '/products/list',
      '/products/',  // Get single product
      
      // Banners - active promotions (including promotions)
      '/banners/',  // All banner endpoints
      '/promotions/',  // All promotion endpoints
      
      // Health checks
      '/health',
      '/gateway/status',
    ];

    // Check each route
    for (const route of publicRoutes) {
      if (route.endsWith('/')) {
        // Prefix match for routes ending with /
        if (pathForComparison.startsWith(route)) {
          console.log(`[Gateway] ✅ PUBLIC (prefix): ${inputPath} → matches ${route}`);
          return true;
        }
      } else {
        // Exact or wildcard match for specific routes
        if (pathForComparison === route || 
            pathForComparison.startsWith(route + '/') || 
            pathForComparison.startsWith(route + '?')) {
          console.log(`[Gateway] ✅ PUBLIC (exact): ${inputPath} → matches ${route}`);
          return true;
        }
      }
    }

    console.log(`[Gateway] 🔒 PROTECTED: ${inputPath} - requires JWT auth`);
    return false;
  }

  /**
   * Check rate limit for a user/IP
   */
  async checkRateLimit(identifier: string, path: string): Promise<{ allowed: boolean; remaining: number; limit: number }> {
    try {
      const normalizedPath = String(path || '')
        .trim()
        .replace(/\/+$/, '')
        .replace(/^\/api/, '') || '';

      const rateLimitBypassPaths = new Set([
        '/users/login',
        '/users/register',
        '/auth/login',
        '/auth/register',
        '/merchants/login',
        '/merchants/register',
        '/merchant/login',
        '/merchant/register',
      ]);

      if (rateLimitBypassPaths.has(normalizedPath)) {
        return { allowed: true, remaining: Number.MAX_SAFE_INTEGER, limit: Number.MAX_SAFE_INTEGER };
      }

      // If Redis not connected, allow all requests
      if (!this.redisService.isEnabled()) {
        return { allowed: true, remaining: 99, limit: 100 };
      }

      // Get rate limit config for this path
      const config = this.rateLimitConfigs.get(path) || this.rateLimitConfigs.get('default');
      const windowMs = config.windowMs;
      const maxRequests = config.maxRequests;

      // Redis key for this user
      const countKey = `${this.redisKeyPrefix}rate-limit-count:${identifier}`;
      const client = this.redisService.getClient();

      if (!client) {
        return { allowed: true, remaining: maxRequests, limit: maxRequests };
      }

      // Get current count using async/await
      let count = await client.incr(countKey);

      // Set expiration on first request
      if (count === 1) {
        await client.expire(countKey, Math.ceil(windowMs / 1000));
      }

      const allowed = count <= maxRequests;
      const remaining = Math.max(0, maxRequests - count);

      return {
        allowed,
        remaining,
        limit: maxRequests
      };
    } catch (error) {
      // On error, allow request
      return { allowed: true, remaining: 100, limit: 100 };
    }
  }

  /**
   * Log request details
   */
  logRequest(context: RequestContext, statusCode: number, duration: number): void {
    const logData = {
      timestamp: new Date().toISOString(),
      userId: context.userId || 'anonymous',
      path: context.path,
      method: context.method,
      statusCode,
      duration: `${duration}ms`,
      ip: context.ip
    };

    if (statusCode >= 400) {
      console.warn('[GATEWAY] Error Request:', logData);
    } else if (duration > 1000) {
      console.warn('[GATEWAY] Slow Request:', logData);
    } else {
      console.debug('[GATEWAY] Request:', logData);
    }
  }

  /**
   * Get gateway health status
   */
  async getGatewayHealth(): Promise<any> {
    return {
      status: 'healthy',
      timestamp: new Date().toISOString(),
      uptime: process.uptime(),
      redis: this.redisService.isEnabled() ? 'connected' : 'disconnected',
      version: '1.0.0'
    };
  }

  /**
   * Get rate limit stats for a user
   */
  async getRateLimitStats(identifier: string): Promise<any> {
    try {
      if (!this.redisService.isEnabled()) {
        return { available: true };
      }

      const countKey = `${this.redisKeyPrefix}rate-limit-count:${identifier}`;
      const client = this.redisService.getClient();

      if (!client) {
        return { count: 0 };
      }

      const count = await client.get(countKey);
      if (!count || typeof count !== 'string') {
        return { count: 0 };
      }

      const ttl = await client.ttl(countKey);
      return {
        count: parseInt(count, 10),
        expiresIn: ttl,
        resetAt: new Date(Date.now() + Math.max(0, ttl) * 1000).toISOString()
      };
    } catch (error: any) {
      return { error: error.message };
    }
  }
}
